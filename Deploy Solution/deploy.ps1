<#
 .SYNOPSIS
    Uses a template to deploy the solution to Azure

 .DESCRIPTION
    Deploys an Azure Resource Manager template

 .PARAMETER subscriptionId
    The subscription id where the template will be deployed.

 .PARAMETER resourceGroupName
    The resource group where the template will be deployed. Can be the name of an existing or a new resource group.

 .PARAMETER resourceGroupLocation
    Optional, a resource group location. If specified, will try to create a new resource group in this location. If not specified, assumes resource group is existing.

 .PARAMETER templateFilePath
    Optional, path to the template file. Defaults to template.json.
#>

param(
 [Parameter(Mandatory=$True)]
 [string]
 $resourceGroupName,

 [Parameter(Mandatory=$True, HelpMessage="Value will be used to name azure resources. Use only lowercase letters.")]
 [string]
 $name_prefix,

 [string]
 $subscriptionId,

 [string]
 $resourceGroupLocation,


 [string]
 $templateFilePath = "template.json"
)

<#
.SYNOPSIS
    Registers RPs
#>
Function RegisterRP {
    Param(
        [string]$ResourceProviderNamespace
    )

    Write-Host "Registering resource provider '$ResourceProviderNamespace'";
    Register-AzureRmResourceProvider -ProviderNamespace $ResourceProviderNamespace;
}

#******************************************************************************
# Script body
# Execution begins here
#******************************************************************************
$ErrorActionPreference = "Stop"

Write-Host "`n`n********************************************************************************" -ForegroundColor Green
Write-Host "`n`tAzure_ConsumeStoreProcess_ApiData" -ForegroundColor Green
Write-Host "`n`tDeploy a solution in Azure that demonstrates how data from a public" -ForegroundColor Green
Write-Host "`tAPI can be ingested into DocumentDB and processed with Cognitive" -ForegroundColor Green
Write-Host "`tServices.`n" -ForegroundColor Green
Write-Host "********************************************************************************`n`n" -ForegroundColor Green


# If the user isn't currently signed in to Azure, ask for credentials
$accountContext = Get-AzureRmContext
if(!$accountContext.Account) {
	Write-Host "Logging in...";
	Login-AzureRmAccount;
}

if($PSBoundParameters.ContainsKey('subscriptionId')) {
	# select subscription if one was specified
	Write-Host "Selecting subscription '$subscriptionId'";
	Select-AzureRmSubscription -SubscriptionId $subscriptionId;
}
else {
	$currentSubscription = (Get-AzureRmContext).Subscription;
	Write-Host "Using subscription ""$($currentSubscription.Name)"" (ID $($currentSubscription.Id))";
	$subscriptionId = $currentSubscription.Id;
}

# Register RPs
$resourceProviders = @("microsoft.cognitiveservices","microsoft.documentdb","microsoft.storage","microsoft.web");
if($resourceProviders.length) {
    Write-Host "Registering resource providers"
    foreach($resourceProvider in $resourceProviders) {
        RegisterRP($resourceProvider);
    }
}

#Create or check for existing resource group
$resourceGroup = Get-AzureRmResourceGroup -Name $resourceGroupName -ErrorAction SilentlyContinue
if(!$resourceGroup) {
    Write-Host "Resource group '$resourceGroupName' does not exist. It will be created.";
    if(!$resourceGroupLocation) {
		Write-Host "To create a new resource group, please enter a location.";
        $resourceGroupLocation = Read-Host "resourceGroupLocation";
    }
    Write-Host "Creating resource group '$resourceGroupName' in location '$resourceGroupLocation'";
    New-AzureRmResourceGroup -Name $resourceGroupName -Location $resourceGroupLocation
}
else {
    Write-Host "Using existing resource group '$($resourceGroup.ResourceGroupName)' in location '$($resourceGroup.Location)'";
}

# Create a template parameter object
$templateParameters = @{
	name_prefix = $name_prefix
	location = $resourceGroupLocation
};

# Start the deployment
Write-Host "Starting deployment...";
$result = New-AzureRmResourceGroupDeployment -ResourceGroupName $resourceGroupName -TemplateFile $templateFilePath -TemplateParameterObject $templateParameters ;
Write-Host "Deployment complete!`n";

# Package up the output of the ARM template into a tidy little object
$templateOutputValues = @{
	databaseAccountName = $result.Outputs.Item("databaseAccountName").Value
	storageAccountName = $result.Outputs.Item("storageAccountName").Value
	functionsSiteName = $result.Outputs.Item("functionsSiteName").Value
};

# Get storage account details
$storageAccountKeys = Get-AzureRmStorageAccountKey -ResourceGroupName $resourceGroupName -AccountName $templateOutputValues.storageAccountName;
$storageAccountKey = $storageAccountKeys[0].Value;
$storageContext = New-AzureStorageContext -StorageAccountName $templateOutputValues.storageAccountName -StorageAccountKey $storageAccountKey;

# Create storage queues needed for the solution
Write-Host "Creating storage queues...";
$queueNames = "-agency", "-doc-add", "-doc-ai", "-doc-score-category", "-doc-score-rating";
$queueNames = $queueNames | ForEach-Object { $_ = $resourceGroupName.ToLower() + $_ ; $_ };
$queueNames | New-AzureStorageQueue -Context $storageContext | out-null
Write-Host "`n"

# Invoke another script that will use REST APIs to create the DocumentDB
Write-Host "`n--------------------------------------------------------------------------------";
./createDocdbCollections.ps1 -resourceGroupName $resourceGroupName -databaseAccountName $templateOutputValues.databaseAccountName;
Write-Host "--------------------------------------------------------------------------------`n";


# Invoke another script that upload the source code for the Azure Functions
Write-Host "`n--------------------------------------------------------------------------------";
./uploadAzureFunctionCode.ps1 -resourceGroupName $resourceGroupName -functionsSiteName $templateOutputValues.functionsSiteName;
Write-Host "--------------------------------------------------------------------------------`n";
