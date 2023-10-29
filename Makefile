deploy:
	databricks_host=${databricks_host} databricks_repo_id=${databricks_repo_id} databricks_token=${databricks_token} deploy_path=${deploy_path} bash cicd/scripts/deploy.sh

push:
	git push gh_origin
	git push origin