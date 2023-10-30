echo "1" "Configuring Databricks CLI"
echo databricks configure --host ${databricks_host} --token-file .databrickscfg

echo "2" "Checking Out 'develop' Branch"
databricks repos update --branch develop --repo-id ${databricks_repo_id}

echo "3" "Cleaning Old Release"
FOLDER=${deploy_path}
folder=$(databricks workspace ls --id ${FOLDER})
if [[ "$folder" = Error* ]] ; then
  echo "Folder $FOLDER not found. Skipping..."
else
  echo "Deleting $FOLDER"
  databricks workspace rm $FOLDER --recursive
fi

echo "4" "Publishing New Release"
FOLDER=${deploy_path}
echo $FOLDER
databricks workspace import_dir az_databricks $FOLDER --exclude-hidden-files
databricks workspace rm --recursive $FOLDER/lc_utils.egg-info

