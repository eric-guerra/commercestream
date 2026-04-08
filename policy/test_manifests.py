"""Static checks over the deployment manifests: production-readiness rules that are easy to forget and
cross-file invariants (e.g. the autoscaler must never exceed the topic's partition count)."""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent / "deploy"


def docs(*rel):
    out = []
    for r in rel:
        for p in sorted((ROOT / r).glob("*.yaml")):
            out += [d for d in yaml.safe_load_all(p.read_text()) if d and d.get("kind") != "Kustomization"]
    return out


ALL = docs("base", "platform", "argocd")
by_kind = lambda k: [d for d in ALL if d["kind"] == k]


def test_every_file_parses_and_has_kind():
    assert ALL and all("kind" in d and "metadata" in d for d in ALL)


def test_deployments_are_production_ready():
    for d in by_kind("Deployment"):
        pod = d["spec"]["template"]["spec"]
        assert pod["securityContext"]["runAsNonRoot"] is True
        assert d["spec"]["strategy"]["rollingUpdate"]["maxUnavailable"] == 0
        for c in pod["containers"]:
            assert c["resources"]["requests"]["cpu"] and c["resources"]["limits"]["memory"], c["name"]
            assert "livenessProbe" in c and "readinessProbe" in c
            sc = c["securityContext"]
            assert sc["readOnlyRootFilesystem"] and not sc["allowPrivilegeEscalation"] and sc["capabilities"]["drop"] == ["ALL"]
            assert not c["image"].endswith(":latest") and ":" in c["image"]


def test_every_deployment_has_a_pdb_and_networkpolicy():
    deps = {d["metadata"]["name"] for d in by_kind("Deployment")}
    assert deps <= {p["metadata"]["name"] for p in by_kind("PodDisruptionBudget")}
    assert deps <= {p["metadata"]["name"] for p in by_kind("NetworkPolicy")}


def test_autoscaler_never_exceeds_partitions():
    partitions = {t["metadata"]["name"]: t["spec"]["partitions"] for t in by_kind("KafkaTopic")}
    for so in by_kind("ScaledObject"):
        topic = so["spec"]["triggers"][0]["metadata"]["topic"]
        assert so["spec"]["maxReplicaCount"] <= partitions[topic], "extra consumers would sit idle"


def test_topics_are_durable_and_kafka_matches():
    for t in by_kind("KafkaTopic"):
        assert t["spec"]["replicas"] >= 3 and t["spec"]["config"]["min.insync.replicas"] == "2", t["metadata"]["name"]
        assert t["metadata"]["labels"]["strimzi.io/cluster"] == "commerce"
    state = next(t for t in by_kind("KafkaTopic") if t["metadata"]["name"].endswith(".state.v1"))
    assert state["spec"]["config"]["cleanup.policy"] == "compact"
    kafka = by_kind("Kafka")[0]["spec"]["kafka"]["config"]
    assert kafka["transaction.state.log.replication.factor"] == 3 and kafka["auto.create.topics.enable"] is False


def test_argocd_leaves_replicas_to_keda():
    tmpl = by_kind("ApplicationSet")[0]["spec"]["template"]["spec"]
    assert {"group": "apps", "kind": "Deployment", "jsonPointers": ["/spec/replicas"]} in tmpl["ignoreDifferences"]
    assert tmpl["syncPolicy"]["automated"]["selfHeal"] is True


def test_overlay_patches_target_real_objects():
    names = {(d["kind"], d["metadata"]["name"]) for d in docs("base")}
    for env in ("staging", "prod"):
        k = yaml.safe_load((ROOT / "overlays" / env / "kustomization.yaml").read_text())
        for p in k.get("patches", []):
            assert (p["target"]["kind"], p["target"]["name"]) in names, (env, p["target"])
        assert all(i["newTag"] != "latest" for i in k["images"])
