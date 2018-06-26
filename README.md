# Azure_ConsumeStoreProcess_ApiData
This project demonstrates how you can use the Microsoft Azure platform to consume data from a public API, store the data,
and do advanced processing on the data.

This solution illustrates the usage of some key Azure technologies:
  * Azure Functions
  * Azure DocumentDB
  * Azure App Service (with a Node.js web application)
  * Azure Resource Manager templates and PowerShell commands to deploy a complete solution

## How to Deploy the Solution
After downloading the code from GitHub, you can run the "deploy.ps1" PowerShell script in the "Deploy Solution" folder.  Use
something like this:
```
.\deploy.ps1 -resourceGroupName "fedreg" -resourceGroupLocation "eastus"
```

This will create a new resource group in the current Azure subscription and populate it with the necessary Azure resources.
(If you wish to use a particular subscription, the deployment script supports a "SubscriptionId" parameter.)
