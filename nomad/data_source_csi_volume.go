package nomad

import (
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/nomad/api"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func dataSourceCSIVolume() *schema.Resource {
	return &schema.Resource{
		Read: dataSourceCSIVolumeRead,
		Schema: map[string]*schema.Schema{

			"volume_id": {
				Description: "Volume ID",
				Type:        schema.TypeString,
				Required:    true,
			},
			"namespace": {
				Description: "Volume Namespace",
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "default",
			},
			// computed attributes
			"name": {
				Description: "Volume Name",
				Type:        schema.TypeString,
				Computed:    true,
			},
			"type": {
				Description: "Volume Type",
				Type:        schema.TypeString,
				Computed:    true,
			},
			"external_id": {
				Description: "Volume External Id",
				Type:        schema.TypeInt,
				Computed:    true,
			},
			"plugin_id": {
				Description: "Volume Plugin Id",
				Type:        schema.TypeString,
				Computed:    true,
			},
			"access_mode": {
				Description: "Volume Access Mode",
				Type:        schema.TypeString,
				Computed:    true,
			},
			"attachment_mode": {
				Description: "Volume Attachement Mode",
				Type:        schema.TypeString,
				Computed:    true,
			},
			"mount_options": {
				Description: "Volume Mount Options",
				Type:        schema.TypeMap,
				Elem:        schema.TypeString,
				Computed:    true,
			},
			"create_index": {
				Description: "Volume Create Index",
				Type:        schema.TypeInt,
				Computed:    true,
			},
			"modify_index": {
				Description: "Volume Modify Index",
				Type:        schema.TypeInt,
				Computed:    true,
			},
		},
	}
}

func dataSourceCSIVolumeRead(d *schema.ResourceData, meta interface{}) error {
	providerConfig := meta.(ProviderConfig)
	client := providerConfig.client

	id := d.Get("volume_id").(string)
	ns := d.Get("namespace").(string)
	if ns == "" {
		ns = "default"
	}
	log.Printf("[DEBUG] Getting volume status: %q/%q", ns, id)
	volume, _, err := client.CSIVolumes().Info(id, &api.QueryOptions{
		Namespace: ns,
	})
	if err != nil {
		// As of Nomad 0.4.1, the API client returns an error for 404
		// rather than a nil result, so we must check this way.
		if strings.Contains(err.Error(), "404") {
			return err
		}

		return fmt.Errorf("error checking for job: %#v", err)
	}

	d.SetId(volume.ID)
	d.Set("volume_id", volume.ID)
	d.Set("namespace", volume.Namespace)
	d.Set("name", volume.Name)
	d.Set("type", "csi")
	d.Set("external_id", volume.ExternalID)
	d.Set("plugin_id", volume.PluginID)
	d.Set("access_mode", volume.AccessMode)
	d.Set("attachment_mode", volume.AttachmentMode)
	d.Set("mount_options", volume.MountOptions) //TODO is map
	d.Set("create_index", volume.CreateIndex)
	d.Set("modify_index", volume.ModifyIndex)

	return nil
}
