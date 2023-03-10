cd terraform/
if [ -e my-deployment-package.zip ]
then 
    rm -rf my-deployment-package.zip
fi

# target has to be /python/lib/python3.8/site-packages for aws lambda layer 
pip install --target ./python/lib/python3.8/site-packages --no-cache-dir -r ../requirements.txt
zip -r my-deployment-package.zip python
rm -rf python