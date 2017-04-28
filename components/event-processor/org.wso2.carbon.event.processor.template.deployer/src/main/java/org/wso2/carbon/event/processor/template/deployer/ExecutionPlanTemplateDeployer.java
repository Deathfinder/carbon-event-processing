/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.carbon.event.processor.template.deployer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.context.PrivilegedCarbonContext;
import org.wso2.carbon.event.processor.core.exception.ExecutionPlanConfigurationException;
import org.wso2.carbon.event.processor.core.exception.ExecutionPlanDependencyValidationException;
import org.wso2.carbon.event.processor.core.internal.util.EventProcessorConstants;
import org.wso2.carbon.event.processor.template.deployer.internal.ExecutionPlanDeployerValueHolder;
import org.wso2.carbon.event.processor.template.deployer.internal.util.ExecutionPlanDeployerConstants;
import org.wso2.carbon.event.processor.template.deployer.internal.util.ExecutionPlanDeployerHelper;
import org.wso2.carbon.event.template.manager.core.DeployableTemplate;
import org.wso2.carbon.event.template.manager.core.TemplateDeployer;
import org.wso2.carbon.event.template.manager.core.TemplateDeploymentException;
import org.wso2.carbon.event.template.manager.core.structure.configuration.ScenarioConfiguration;
import org.wso2.carbon.registry.core.Collection;
import org.wso2.carbon.registry.core.Registry;
import org.wso2.carbon.registry.core.RegistryConstants;
import org.wso2.carbon.registry.core.exceptions.RegistryException;
import org.wso2.siddhi.query.api.util.AnnotationHelper;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

public class ExecutionPlanTemplateDeployer implements TemplateDeployer {

    private static final Log log = LogFactory.getLog(ExecutionPlanTemplateDeployer.class);

    @Override
    public String getType() {
        return "realtime";
    }

    @Override
    public void deployArtifact(DeployableTemplate template) throws TemplateDeploymentException {
        try {
            if (template == null) {
                throw new TemplateDeploymentException("No artifact received to be deployed.");
            }
            int tenantId = PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantId();
            Registry registry = ExecutionPlanDeployerValueHolder.getRegistryService()
                    .getConfigSystemRegistry(tenantId);
            if (!registry.resourceExists(ExecutionPlanDeployerConstants.META_INFO_COLLECTION_PATH)) {
                registry.put(ExecutionPlanDeployerConstants.META_INFO_COLLECTION_PATH, registry.newCollection());
            }

            Collection infoCollection = registry.get(ExecutionPlanDeployerConstants.META_INFO_COLLECTION_PATH, 0, -1);
            String artifactId = template.getArtifactId();
            artifactId = artifactId.replaceAll(" ", "_");
            String planId = infoCollection.getProperty(artifactId);

            //~~~~~~~~~~~~~Cleaning up previously deployed plan, if any.

            if (planId != null) {
                infoCollection.removeProperty(artifactId);
                registry.put(ExecutionPlanDeployerConstants.META_INFO_COLLECTION_PATH, infoCollection);

                //Checking whether any other scenario configs/domains are using this stream....
                //this info is being kept in a map
                String mappingResourcePath = ExecutionPlanDeployerConstants.META_INFO_COLLECTION_PATH + RegistryConstants.PATH_SEPARATOR + planId;
                if (registry.resourceExists(mappingResourcePath)) {
                    ExecutionPlanDeployerHelper.cleanMappingResourceAndUndeploy(registry, mappingResourcePath, artifactId, planId);
                }
            }

            //~~~~~~~~~~~~~Deploying new plan
            String executionPlan = template.getArtifact();
            org.wso2.siddhi.query.api.ExecutionPlan parsedExecutionPlan = SiddhiCompiler.parse(executionPlan);
            planId = AnnotationHelper.getAnnotationElement(EventProcessorConstants.ANNOTATION_NAME_NAME, null, parsedExecutionPlan.getAnnotations()).getValue();

            String planName = getPlanName(template.getConfiguration(), planId);

            //@Plan:name will be updated with given configuration name and uncomment in case if it is commented
            String executionPlanNameDefinition = ExecutionPlanDeployerConstants.EXECUTION_PLAN_NAME_ANNOTATION + "('"
                    + planName + "')";
            executionPlan = executionPlan.replaceAll(
                    ExecutionPlanDeployerConstants.EXECUTION_PLAN_NAME_ANNOTATION
                            + ExecutionPlanDeployerConstants.REGEX_NAME_COMMENTED_VALUE,
                    executionPlanNameDefinition);

            ExecutionPlanDeployerHelper.updateRegistryMaps(registry, infoCollection, artifactId, planName);
            //Get Template Execution plan, Tenant Id and deploy Execution Plan
            ExecutionPlanDeployerValueHolder.getEventProcessorService().deployExecutionPlan(executionPlan);
        } catch (RegistryException e) {
            throw new TemplateDeploymentException("Could not load the Registry for Tenant Domain: "
                    + PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantDomain(true)
                    + ", when trying to undeploy Event Stream with artifact ID: " + template.getArtifactId(), e);
        } catch (SiddhiParserException e) {
            throw new TemplateDeploymentException(
                    "Validation exception occurred when parsing Execution Plan of Template "
                            + template.getConfiguration().getName() + " of Domain " + template.getConfiguration().getDomain(), e);
        } catch (ExecutionPlanConfigurationException e) {
            throw new TemplateDeploymentException(
                    "Configuration exception occurred when adding Execution Plan of Template "
                    + template.getConfiguration().getName() + " of Domain " + template.getConfiguration().getDomain(), e);

        } catch (ExecutionPlanDependencyValidationException e) {
            throw new TemplateDeploymentException(
                    "Validation exception occurred when adding Execution Plan of Template "
                    + template.getConfiguration().getName() + " of Domain " + template.getConfiguration().getDomain(), e);
        }
    }

    private String getPlanName(ScenarioConfiguration configuration, String planId) {
        return configuration.getDomain() + ExecutionPlanDeployerConstants.PLAN_NAME_SEPARATOR
                        + configuration.getScenario() + ExecutionPlanDeployerConstants.PLAN_NAME_SEPARATOR
                        + configuration.getName()+ ExecutionPlanDeployerConstants.PLAN_NAME_SEPARATOR
                        + planId;
    }

    @Override
    public void deployIfNotDoneAlready(DeployableTemplate template) throws TemplateDeploymentException{
        if (template == null) {
            throw new TemplateDeploymentException("No artifact received to be deployed.");
        }
        String artifactId = template.getArtifactId();
        artifactId = artifactId.replaceAll(" ", "_");
        if (ExecutionPlanDeployerValueHolder.getEventProcessorService().
                getAllActiveExecutionConfigurations().get(artifactId) == null) {
            deployArtifact(template);
        } else {
            if(log.isDebugEnabled()) {
                log.debug("Common Artifact: " + artifactId + " of Domain " + template.getConfiguration().getDomain()
                          + " was not deployed as it is already being deployed.");
            }
        }
    }

    @Override
    public void undeployArtifact(String artifactId) throws TemplateDeploymentException {
        artifactId = artifactId.replaceAll(" ", "_");

        try {
            int tenantId = PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantId();
            Registry registry = ExecutionPlanDeployerValueHolder.getRegistryService()
                    .getConfigSystemRegistry(tenantId);
            if (!registry.resourceExists(ExecutionPlanDeployerConstants.META_INFO_COLLECTION_PATH)) {
                registry.put(ExecutionPlanDeployerConstants.META_INFO_COLLECTION_PATH, registry.newCollection());
            }

            Collection infoCollection = registry.get(ExecutionPlanDeployerConstants.META_INFO_COLLECTION_PATH, 0, -1);
            String planId = infoCollection.getProperty(artifactId);

            if (planId != null) {
                infoCollection.removeProperty(artifactId);
                registry.put(ExecutionPlanDeployerConstants.META_INFO_COLLECTION_PATH, infoCollection);

                String mappingResourcePath = ExecutionPlanDeployerConstants.META_INFO_COLLECTION_PATH + RegistryConstants.PATH_SEPARATOR + planId;
                if (registry.resourceExists(mappingResourcePath)) {
                    ExecutionPlanDeployerHelper.cleanMappingResourceAndUndeploy(registry, mappingResourcePath, artifactId, planId);
                } else {
                    log.warn("Registry data in inconsistent. Resource '" + mappingResourcePath + "' which needs to be deleted is not found.");
                }
            } else {
                log.warn("Registry data in inconsistent. No plan ID associated to artifact ID: " + artifactId + ". Hence nothing to be undeployed.");
            }
        } catch (RegistryException e) {
            throw new TemplateDeploymentException("Could not load the Registry for Tenant Domain: "
                    + PrivilegedCarbonContext.getThreadLocalCarbonContext().getTenantDomain(true)
                    + ", when trying to undeploy Execution Plan with artifact ID: " + artifactId, e);
        }
    }
}
