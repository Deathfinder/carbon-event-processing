package org.wso2.carbon.event.processor.template.deployer.internal.util;

import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.event.processor.core.exception.ExecutionPlanConfigurationException;
import org.wso2.carbon.event.processor.core.internal.util.EventProcessorConstants;
import org.wso2.carbon.event.processor.template.deployer.internal.ExecutionPlanDeployerValueHolder;
import org.wso2.carbon.event.template.manager.core.TemplateDeploymentException;
import org.wso2.carbon.registry.core.Collection;
import org.wso2.carbon.registry.core.Registry;
import org.wso2.carbon.registry.core.RegistryConstants;
import org.wso2.carbon.registry.core.Resource;
import org.wso2.carbon.registry.core.exceptions.RegistryException;

public class ExecutionPlanDeployerHelper {

    public static void updateRegistryMaps(Registry registry, Collection infoCollection,
                                          String artifactId, String planId)
            throws RegistryException {
        infoCollection.addProperty(artifactId, planId);
        registry.put(ExecutionPlanDeployerConstants.META_INFO_COLLECTION_PATH, infoCollection);

        Resource mappingResource;
        String mappingResourceContent = null;
        String mappingResourcePath = ExecutionPlanDeployerConstants.META_INFO_COLLECTION_PATH + RegistryConstants.PATH_SEPARATOR + planId;

        if (registry.resourceExists(mappingResourcePath)) {
            mappingResource = registry.get(mappingResourcePath);
            mappingResourceContent = new String((byte[]) mappingResource.getContent());
        } else {
            mappingResource = registry.newResource();
        }

        if (mappingResourceContent == null) {
            mappingResourceContent = artifactId;
        } else {
            mappingResourceContent += ExecutionPlanDeployerConstants.META_INFO_PLAN_NAME_SEPARATOR + artifactId;
        }

        mappingResource.setMediaType("text/plain");
        mappingResource.setContent(mappingResourceContent);
        registry.put(mappingResourcePath, mappingResource);
    }


    public static void cleanMappingResourceAndUndeploy(Registry registry,
                                                       String mappingResourcePath,
                                                       String artifactId, String planId)
            throws TemplateDeploymentException {
        try {
            Resource mappingResource = registry.get(mappingResourcePath);
            String mappingResourceContent = new String((byte[]) mappingResource.getContent());

            //Removing artifact ID, along with separator comma.
            int beforeCommaIndex = mappingResourceContent.indexOf(artifactId) - 1;
            int afterCommaIndex = mappingResourceContent.indexOf(artifactId) + artifactId.length();
            if (beforeCommaIndex > 0) {
                mappingResourceContent = mappingResourceContent.replace(
                        ExecutionPlanDeployerConstants.META_INFO_PLAN_NAME_SEPARATOR + artifactId, "");
            } else if (afterCommaIndex < mappingResourceContent.length()) {
                mappingResourceContent = mappingResourceContent.replace(
                        artifactId + ExecutionPlanDeployerConstants.META_INFO_PLAN_NAME_SEPARATOR, "");
            } else {
                mappingResourceContent = mappingResourceContent.replace(artifactId, "");
            }

            if (mappingResourceContent.equals("")) {
                //deleting mappingResource
                registry.delete(mappingResourcePath);
            } else {
                mappingResource.setContent(mappingResourceContent);
                registry.put(mappingResourcePath, mappingResource);
            }
            //undeploying existing stream
            if (ExecutionPlanDeployerValueHolder.getEventProcessorService().getAllActiveExecutionConfigurations().keySet().contains(planId)) {
                ExecutionPlanDeployerValueHolder.getEventProcessorService().undeployActiveExecutionPlan(planId);
            } else {
                ExecutionPlanDeployerValueHolder.getEventProcessorService().undeployInactiveExecutionPlan(planId + EventProcessorConstants.EP_CONFIG_FILE_EXTENSION_WITH_DOT);
            }
        } catch (RegistryException e) {
            throw new TemplateDeploymentException("Could not load the Registry for Tenant Domain: "
                                                  + PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantDomain(true)
                                                  + ", when trying to undeploy Event Stream with artifact ID: " + artifactId, e);
        } catch (ExecutionPlanConfigurationException e) {
            throw new TemplateDeploymentException("Could not undeploy execution plan: " + artifactId, e);
        }

    }
}
