import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(
        features = {"src/test/resources/Exercice1.feature"},
        plugin = ("json:target/cucumber-reports/CucumberTestReport.json"),
        monochrome= true,
        dryRun= false
)
class RunCukeTests {

}
