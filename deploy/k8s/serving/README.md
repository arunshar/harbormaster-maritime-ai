# deploy/k8s/serving

This directory holds the EKS serving front door that one bounded demonstration window used. That optional EKS profile ran once, and the cluster was then removed. ECS Fargate remains the serving front door, as the [Production V1 front-door ADR](../../../docs/adr/ADR_PRODUCTION_V1_ECS_FRONT_DOOR.md) explains.

The manifests run the same container image as the ECS Fargate service (`serving/Dockerfile`, unmodified) as a `Deployment` and a fixed `NodePort` Service. A KEDA `ScaledObject` scales the Deployment and sets `minReplicaCount: 0`, which is a pod scale-to-zero floor that a bare HPA cannot express. Terraform owns the internal NLB, its instance target group, and the API Gateway listener wiring in `modules/eks_frontdoor`.

The directory has three parts.

- `base/` is complete on its own. It holds the namespace, the deployment, the service, and the ScaledObject with the Kinesis lag trigger. That trigger is an `aws-cloudwatch` trigger on `GetRecords.IteratorAgeMilliseconds` for the `ais-raw` stream. The note in `base/scaledobject-kinesis.yaml` explains why the manifests do not use the `aws-kinesis-stream` scaler, which scales on shard count and can never reach zero.
- `with-cdc/` adds the Kafka trigger for CDC consumer-group lag (group `hm-cdc-consumer`). It is a JSON6902 patch that appends to the same ScaledObject, because KEDA rejects a second ScaledObject on one `scaleTargetRef`. Apply it only when the `enable_phase2` infrastructure exists, and replace `PLACEHOLDER_MSK_BOOTSTRAP:9098` with the real MSK bootstrap endpoint first.
- `golden/` holds the committed `kubectl kustomize` outputs of both variants. The test `tests/e2e/test_phase5_k8s_manifests.py` rebuilds them and compares them semantically when a kustomize binary is available. It always validates their structure with a pure YAML parse.

Render both variants with these commands.

```bash
kubectl kustomize deploy/k8s/serving/base
kubectl kustomize deploy/k8s/serving/with-cdc
```

The tracked Deployment contains a non-runnable `.invalid` zero-digest sentinel. Render a live manifest only from an immutable ECR digest. The repository commits no account ID and no mutable tag.

```bash
make phase5-render-serving \
  IMAGE=<acct>.dkr.ecr.us-east-1.amazonaws.com/harbormaster-base-serving@sha256:<digest> \
  REPOSITORY=<acct>.dkr.ecr.us-east-1.amazonaws.com/harbormaster-base-serving \
  OUTPUT=/tmp/eks-serving.yaml
```

The ECS Fargate service stays in place while the EKS path runs, so it is the rollback path. The API Gateway proxy route follows the `serving_target` variable in `envs/base`. That variable is `ecs` by default, and it was `eks` only during the EKS demonstration window. The EKS integration URI comes directly from `module.eks_frontdoor[0].listener_arn`, so operators never paste it or look it up by hand. Setting the target back to `ecs` is the rollback, and the Fargate service is still running behind it.

You can check the manifests without AWS. Create a kind cluster, install only the KEDA CRDs, and apply the base manifests. This check needs no KEDA operator, no images, and no cloud account. The API server validates the ScaledObject schema, and the object stays inert without the operator.

```bash
kind create cluster --name hm-phase5-dryrun
kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.20.0/keda-2.20.0-crds.yaml
kubectl apply -k deploy/k8s/serving/base
kubectl -n hm-serving get scaledobject serving-scaler -o yaml
kind delete cluster --name hm-phase5-dryrun
```
