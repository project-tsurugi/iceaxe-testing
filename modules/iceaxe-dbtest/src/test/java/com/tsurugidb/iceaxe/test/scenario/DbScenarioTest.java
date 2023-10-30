package com.tsurugidb.iceaxe.test.scenario;

import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.SelectClasspathResource;


@Suite
@IncludeEngines("cucumber")
@SelectClasspathResource("com/tsurugidb/iceaxe/test/scenario")
@ConfigurationParameter(key = io.cucumber.junit.platform.engine.Constants.GLUE_PROPERTY_NAME, value = "com.tsurugidb.iceaxe.test.scenario")
public class DbScenarioTest {
}
