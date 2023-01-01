cd terraform/
if [ -e my-deployment-package.zip ]
then 
    rm -rf my-deployment-package.zip
fi

pip install --target ./package --no-cache-dir -r ../requirements.txt
zip -r my-deployment-package.zip package
rm -rf package