package k8s

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadPodTemplate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.yaml")
	if err := os.WriteFile(path, []byte(`
metadata:
  labels:
    app.kubernetes.io/name: agentlet
spec:
  containers:
    - name: agentlet
      image: example.test/agentlet:latest
`), 0o600); err != nil {
		t.Fatal(err)
	}

	template, err := LoadPodTemplate(path)
	if err != nil {
		t.Fatal(err)
	}
	if template.Labels["app.kubernetes.io/name"] != "agentlet" ||
		len(template.Spec.Containers) != 1 || template.Spec.Containers[0].Name != "agentlet" {
		t.Fatalf("template = %#v", template)
	}
}

func TestLoadPodTemplateRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.yaml")
	if err := os.WriteFile(path, []byte("spec:\n  containerz: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPodTemplate(path); err == nil {
		t.Fatal("LoadPodTemplate() error = nil, want strict decode error")
	}
}
