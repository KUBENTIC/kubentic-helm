
## Future publish workflow (one-liner):
  
  
  helm package helm/ --destination /tmp/helm-release/ && \
  git checkout gh-pages && \
  cp /tmp/helm-release/*.tgz . && \
  helm repo index . --url https://helm.kubentic.ai --merge index.yaml && \
  git add *.tgz index.yaml && \
  git commit -m "publish chart vX.Y.Z" && \
  git push origin gh-pages && \
  git checkout develop
