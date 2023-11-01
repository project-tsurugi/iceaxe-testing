package com.tsurugidb.iceaxe.test.scenario;

import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.SelectClasspathResource;

// Usage: ./gradlew test --tests DbScenarioTest
//          全シナリオを実行
//          (Gradle が up-to-date と言ってテストを実行しないときは cleanTest test とする)
//        ./gradlew test --tests DbScenarioTest -Dcucumber.filter.tags="@minimal" -Dcucumber.filter.name="normal_order"
//          cucumber.filter.tags シナリオについているタグによる絞り込み
//          cucumber.filter.name シナリオ名による絞り込み
//  see: https://github.com/cucumber/cucumber-jvm/tree/main/cucumber-junit-platform-engine#configuration-options
//   動かず: -Dcucumber.features=com/tsurugidb/iceaxe/test/scenario/issue378.feature

@Suite
@IncludeEngines("cucumber")
@SelectClasspathResource("com/tsurugidb/iceaxe/test/scenario")
@ConfigurationParameter(key = io.cucumber.junit.platform.engine.Constants.GLUE_PROPERTY_NAME, value = "com.tsurugidb.iceaxe.test.scenario")
public class DbScenarioTest {
}
