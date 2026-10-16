// k8s-image-pins prints the images k8s/publish-images.sh builds, from the
// version catalog, so the script and ARIES derive the same tags.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/hyscale-lab/aries/pkg/config"
	hermesharness "github.com/hyscale-lab/aries/pkg/harness/hermes"
)

func main() {
	dockerfile := flag.String("hermes-dockerfile", "", "write the Hermes recipe to this path")
	flag.Parse()
	if err := run(*dockerfile); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run prints one KEY=value line per pin. Values never contain a newline.
func run(dockerfile string) error {
	versions, err := config.LoadVersions("configs/versions.json")
	if err != nil {
		return err
	}
	pin := versions.Hermes.OTelPlugin
	plugin := hermesharness.OTelPlugin{RepositoryURL: pin.RepositoryURL, Version: pin.Version, Revision: pin.Revision}
	image, err := hermesharness.LocalImage(versions.Hermes.Image, plugin)
	if err != nil {
		return err
	}
	args, err := hermesharness.OTelBuildArgs(versions.Hermes.Image, plugin)
	if err != nil {
		return err
	}
	if dockerfile != "" {
		if err := os.WriteFile(dockerfile, []byte(hermesharness.OTelDockerfile), 0o644); err != nil {
			return err
		}
	}
	fmt.Printf("BRIDGE_IMAGE=%s\n", versions.Bridge.Image)
	fmt.Printf("BRIDGE_KUBERNETES_BASE_IMAGE=%s\n", versions.Bridge.KubernetesBaseImage)
	fmt.Printf("HERMES_IMAGE=%s\n", image)
	fmt.Printf("HERMES_BASE=%s\n", args["BASE"])
	fmt.Printf("HERMES_OTEL_PLUGIN=%s\n", args["OTEL_PLUGIN"])
	return nil
}
