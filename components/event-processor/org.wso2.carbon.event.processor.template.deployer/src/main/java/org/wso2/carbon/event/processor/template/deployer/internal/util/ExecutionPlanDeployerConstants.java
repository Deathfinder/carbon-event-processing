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
package org.wso2.carbon.event.processor.template.deployer.internal.util;

import org.wso2.carbon.event.template.manager.core.internal.util.TemplateManagerConstants;
import org.wso2.carbon.registry.core.RegistryConstants;

public class ExecutionPlanDeployerConstants {

    public static final String EXECUTION_PLAN_NAME_ANNOTATION = "@Plan:name";

    public static final String REGEX_NAME_COMMENTED_VALUE = "\\(.*?\\)";

    public static final String META_INFO_COLLECTION_PATH = TemplateManagerConstants.DEPLOYER_META_INFO_PATH
            + RegistryConstants.PATH_SEPARATOR
            + ExecutionPlanDeployerConstants.REALTIME_DEPLOYER_TYPE;
    public static final String META_INFO_PLAN_NAME_SEPARATOR = ",";
    public static final String PLAN_NAME_SEPARATOR = "_";

    public static final String REALTIME_DEPLOYER_TYPE = "realtime";
}
