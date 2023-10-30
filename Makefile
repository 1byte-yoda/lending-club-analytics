push:
	git push gh_origin
	git push origin

build_artifacts:
	python setup.py bdist_wheel
	databricks fs cp --overwrite dist/lc_utils-0.1-py3-none-any.whl "dbfs:/Libraries/lc_utils-0.1-py3-none-any.whl"

install_packages:
	databricks libraries install --cluster-id 1029-154030-uzmy74rr --whl "dbfs:/Libraries/lc_utils-0.1-py3-none-any.whl"

deploy:
	make build_artifacts
	make install_packages
	databricks_host=${databricks_host} databricks_repo_id=${databricks_repo_id} databricks_token=${databricks_token} deploy_path=${deploy_path} bash cicd/scripts/deploy.sh
	make cleanup

cleanup:
	rm -r ./build ./dist ./az_databricks/lc_utils.egg-info
