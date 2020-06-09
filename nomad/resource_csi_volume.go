package nomad

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/nomad/api"
	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"

	"github.com/terraform-providers/terraform-provider-nomad/nomad/core/jobspec"
)

func resourceCSIVolume() *schema.Resource {
	return &schema.Resource{
		Create: resourceVolumeRegister,
		Update: resourceVolumeRegister,
		Delete: resourceVolumeDeregister,
		Read:   resourceCSIVolumeRead,

		CustomizeDiff: resourceVolumeCustomizeDiff,

		Schema: map[string]*schema.Schema{
			"volumespec": {
				Description:      "Volume specification. If you want to point to a file use the file() function.",
				Required:         true,
				Type:             schema.TypeString,
				DiffSuppressFunc: volumespecDiffSuppress,
			},

			"deregister_on_destroy": {
				Description: "If true, the job will be deregistered on destroy.",
				Optional:    true,
				Default:     true,
				Type:        schema.TypeBool,
			},
		},
	}
}

func resourceVolumeRegister(d *schema.ResourceData, meta interface{}) error {
	providerConfig := meta.(ProviderConfig)
	client := providerConfig.client

	// Get the jobspec itself
	volumespecRaw := d.Get("volumespec").(string)
	is_json := d.Get("json").(bool)
	volume, err := parseVolumespec(volumespecRaw, is_json)
	if err != nil {
		return err
	}

	_, err = client.CSIVolumes().Register(volume, &api.WriteOptions{
		Namespace: d.Get("namespace").(string),
	})

	if err != nil {
		return fmt.Errorf("error applying volumespec: %s", err)
	}

	log.Printf("[DEBUG] volume '%s' registered", volume.ID)
	d.SetId(volume.ID)
	d.Set("name", volume.Name)
	d.Set("namespace", volume.Namespace)
	d.Set("modify_index", strconv.FormatUint(volume.ModifyIndex, 10))

	return resourceCSIVolumeRead(d, meta) // populate other computed attributes
}

// deploymentStateRefreshFunc returns a resource.StateRefreshFunc that is used to watch
// the deployment from a job create/update
func deploymentStateRefreshFunc(client *api.Client, initialEvalId string) resource.StateRefreshFunc {

	// evalId is the evaluation that we are currently monitoring. This will change
	// along with follow-up evaluations.
	evalId := initialEvalId

	// deploymentId is the deployment that we are monitoring. This is captured from the
	// final evaluation.
	deploymentId := ""

	return func() (interface{}, string, error) {
		if deploymentId == "" {
			// monitor the eval
			log.Printf("[DEBUG] monitoring evaluation '%s'", evalId)
			eval, _, err := client.Evaluations().Info(evalId, nil)
			if err != nil {
				log.Printf("[ERROR] error on Evaluation.Info during deploymentStateRefresh: %s", err)
				return nil, "", err
			}

			switch eval.Status {
			case "complete":
				// Monitor the next eval in the chain, if present
				var state string
				if eval.NextEval != "" {
					log.Printf("[DEBUG] will monitor follow-up eval '%v'", evalId)
					evalId = eval.NextEval
					state = "monitoring_evaluation"
				} else if eval.DeploymentID != "" {
					log.Printf("[DEBUG] job has been scheduled, will monitor deployment '%s'", eval.DeploymentID)
					deploymentId = eval.DeploymentID
					state = "monitoring_deployment"
				} else {
					log.Printf("[WARN] job has been scheduled, but there is no deployment to monitor")
					state = "job_scheduled_without_deployment"
				}
				return nil, state, nil
			case "failed", "cancelled":
				return nil, "", fmt.Errorf("evaluation failed: %v", eval.StatusDescription)
			default:
				return nil, "monitoring_evaluation", nil
			}
		} else {
			// monitor the deployment
			var state string
			deployment, _, err := client.Deployments().Info(deploymentId, nil)
			if err != nil {
				log.Printf("[ERROR] error on Deployment.Info during deploymentStateRefresh: %s", err)
				return nil, "", err
			}
			switch deployment.Status {
			case "successful":
				log.Printf("[DEBUG] deployment '%s' successful", deployment.ID)
				state = "deployment_successful"
			case "failed", "cancelled":
				log.Printf("[DEBUG] deployment unsuccessful: %s", deployment.StatusDescription)
				return deployment, "",
					fmt.Errorf("deployment '%s' terminated with status '%s': '%s'",
						deployment.ID, deployment.Status, deployment.StatusDescription)
			default:
				// don't overwhelm the API server
				state = "monitoring_deployment"
			}
			return deployment, state, nil
		}
	}
}

func resourceVolumeDeregister(d *schema.ResourceData, meta interface{}) error {
	providerConfig := meta.(ProviderConfig)
	client := providerConfig.client

	// If deregistration is disabled, then do nothing

	deregister_on_destroy := d.Get("deregister_on_destroy").(bool)
	if !deregister_on_destroy {
		log.Printf(
			"[WARN] volume %q will not deregister since "+
				"'deregister_on_destroy' is %t", d.Id(), deregister_on_destroy)
		return nil
	}

	id := d.Id()
	log.Printf("[DEBUG] deregistering volume: %q", id)
	opts := &api.WriteOptions{
		Namespace: d.Get("namespace").(string),
	}
	if opts.Namespace == "" {
		opts.Namespace = "default"
	}
	err := client.CSIVolumes().Deregister(id, opts)
	if err != nil {
		return fmt.Errorf("error deregistering job: %s", err)
	}

	return nil
}

func resourceCSIVolumeRead(d *schema.ResourceData, meta interface{}) error {
	providerConfig := meta.(ProviderConfig)
	client := providerConfig.client

	id := d.Id()
	opts := &api.QueryOptions{
		Namespace: d.Get("namespace").(string),
	}
	if opts.Namespace == "" {
		opts.Namespace = "default"
	}
	log.Printf("[DEBUG] reading information for volume %q in namespace %q", id, opts.Namespace)
	volume, _, err := client.CSIVolumes().Info(id, opts)
	if err != nil {
		// As of Nomad 0.4.1, the API client returns an error for 404
		// rather than a nil result, so we must check this way.
		if strings.Contains(err.Error(), "404") {
			log.Printf("[DEBUG] volume %q does not exist, so removing", id)
			d.SetId("")
			return nil
		}

		return fmt.Errorf("error checking for job: %s", err)
	}
	log.Printf("[DEBUG] found volume %q in namespace %q", volume.Name, volume.Namespace)

	d.Set("id", volume.ID)
	d.Set("name", volume.Name)
	//d.Set("type", volume.Type) //TODO
	d.Set("external_id", volume.ExternalID)
	d.Set("plugin_id", volume.PluginID)
	d.Set("access_mode", volume.AccessMode)
	d.Set("attachment_mode", volume.AttachmentMode)
	d.Set("mount_options", volume.MountOptions) //TODO
	//d.Set("parameters", volume.Parameters) //TODO
	//d.Set("context", volume.Context) //TODO
	d.Set("namespace", volume.Namespace)

	if volume.ModifyIndex != 0 {
		d.Set("modify_index", strconv.FormatUint(volume.ModifyIndex, 10))
	} else {
		d.Set("modify_index", "0")
	}

	return nil
}

func resourceVolumeCustomizeDiff(d *schema.ResourceDiff, meta interface{}) error {
	log.Printf("[DEBUG] resourceVolumeCustomizeDiff")

	if !d.NewValueKnown("volumespec") {
		d.SetNewComputed("id")
		d.SetNewComputed("modify_index")
		d.SetNewComputed("namespace")
		d.SetNewComputed("type")
		d.SetNewComputed("name")
		d.SetNewComputed("external_id")
		d.SetNewComputed("plugin_id")
		d.SetNewComputed("access_mode")
		d.SetNewComputed("attachment_mode")
		d.SetNewComputed("mount_options")
		return nil
	}

	oldSpecRaw, newSpecRaw := d.GetChange("volumespec")

	if oldSpecRaw.(string) == newSpecRaw.(string) {
		// nothing to do!
		return nil
	}

	is_json := d.Get("json").(bool)
	volume, err := parseVolumespec(newSpecRaw.(string), is_json) // catch syntax errors client-side during plan
	if err != nil {
		return err
	}

	defaultNamespace := "default"
	if volume.Namespace == "" {
		volume.Namespace = defaultNamespace
	}

	d.SetNew("id", volume.ID)
	d.SetNew("name", volume.Name)
	//d.SetNew("type", volume.Type)

	// If the identity has changed and the config asks us to deregister on identity
	// change then the id field "forces new resource".
	if d.Get("namespace").(string) != volume.Namespace {
		log.Printf("[DEBUG] namespace change forces new resource")
		d.SetNew("namespace", volume.Namespace)
		d.ForceNew("namespace")
	} else if d.Id() != volume.ID {
		if d.Get("deregister_on_id_change").(bool) {
			log.Printf("[DEBUG] name change forces new resource because deregister_on_id_change is set")
			d.ForceNew("id")
			d.ForceNew("name")
		} else {
			log.Printf("[DEBUG] allowing name change as update because deregister_on_id_change is not set")
		}
	}
	// TODO: is this needed?
	//else {
	//	d.SetNew("namespace", volume.Namespace)
	//
	//	// If the identity (namespace+name) _isn't_ changing, then we require consistency of the
	//	// job modify index to ensure that the "old" part of our diff
	//	// will show what Nomad currently knows.
	//	wantModifyIndexStr := d.Get("modify_index").(string)
	//	wantModifyIndex, err := strconv.ParseUint(wantModifyIndexStr, 10, 64)
	//	if err != nil {
	//		// should never happen, because we always write with FormatUint
	//		// in Read above.
	//		return fmt.Errorf("invalid modify_index in state: %s", err)
	//	}
	//
	//	if resp != nil && resp.JobModifyIndex != wantModifyIndex {
	//		// Should rarely happen, but might happen if there was a concurrent
	//		// other process writing to Nomad since our Read call.
	//		return fmt.Errorf("job modify index has changed since last refresh")
	//	}
	//}

	// We know that applying changes here will change the modify index
	// _somehow_, but we won't know how much it will increment until
	// after we complete registration.
	d.SetNewComputed("modify_index")
	d.SetNewComputed("allocation_ids")

	return nil
}

func parseVolumespec(raw string, is_json bool) (*api.CSIVolume, error) {
	var volume *api.CSIVolume
	var err error

	if is_json {
		volume, err = parseJSONVolumespec(raw)
	} else {
		return nil, fmt.Errorf("error parsing jobspec: no json string")
	}
	if err != nil {
		return nil, fmt.Errorf("error parsing jobspec: %s", err)
	}

	// If volume is empty after parsing, the input is not a valid Nomad volume.
	if volume == nil || reflect.DeepEqual(volume, &api.CSIVolume{}) {
		return nil, fmt.Errorf("error parsing volumespec: input JSON is not a valid Nomad volumespec")
	}

	return volume, nil
}

func parseJSONVolumespec(raw string) (*api.CSIVolume, error) {
	// `nomad job run -output` returns a jobspec with a "Job" root, so
	// partially parse the input JSON to detect if we have this root.
	var root map[string]json.RawMessage

	err := json.Unmarshal([]byte(raw), &root)
	if err != nil {
		return nil, err
	}

	jobBytes, ok := root["Job"]
	if !ok {
		// Parse the input as is if there's no "Job" root.
		jobBytes = []byte(raw)
	}

	// Parse actual job.
	var volume api.CSIVolume
	err = json.Unmarshal(jobBytes, &volume)
	if err != nil {
		return nil, err
	}

	return &volume, nil
}

// jobspecDiffSuppress is the DiffSuppressFunc used by the schema to
// check if two jobspecs are equal.
func volumespecDiffSuppress(k, old, new string, d *schema.ResourceData) bool {
	// TODO: does this need to consider is_json ???
	// Parse the old job
	oldJob, err := jobspec.Parse(strings.NewReader(old))
	if err != nil {
		return false
	}

	// Parse the new job
	newJob, err := jobspec.Parse(strings.NewReader(new))
	if err != nil {
		return false
	}

	// Init
	oldJob.Canonicalize()
	newJob.Canonicalize()

	// Check for jobspec equality
	return reflect.DeepEqual(oldJob, newJob)
}
