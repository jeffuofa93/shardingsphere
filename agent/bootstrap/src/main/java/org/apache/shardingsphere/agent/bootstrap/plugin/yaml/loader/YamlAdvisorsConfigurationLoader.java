/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.agent.bootstrap.plugin.yaml.loader;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.agent.bootstrap.plugin.yaml.entity.YamlAdvisorsConfiguration;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;

/**
 * YAML advisors configuration loader.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class YamlAdvisorsConfigurationLoader {
    
    /**
     * Load advisors configuration.
     * 
     * @param inputStream input stream
     * @return loaded advisors configuration
     */
    public static YamlAdvisorsConfiguration load(final InputStream inputStream) {
        YamlAdvisorsConfiguration result = new Yaml().loadAs(inputStream, YamlAdvisorsConfiguration.class);
        return null == result ? new YamlAdvisorsConfiguration() : result;
    }
}
