package test.java.stepdefs.SparkSteps;

import cucumber.api.PendingException;
import cucumber.api.java.en.Given;
import cucumber.api.java.en.When;
import cucumber.api.java.en.Then;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

public class SparkStepsDefinitions {

    public SparkContext sc;

    @Given("Le job spark {string} lancé")
    public void le_job_spark_lancé(String string) {
        // Write code here that turns the phrase above into concrete actions
        try {
            sc = new SparkContext(new SparkConf().setMaster("local[1]").setAppName(string));
            System.out.print(sc.appName());
        } catch (Exception e) {
            throw e;
        }
    }

    @Then("Le job spark {string} est stoppé")
    public void le_job_spark_est_stoppé(String string) {
        try {
            System.out.print("avant stop"+sc.applicationId());

        } catch (Exception e) {
            throw e;
        } finally {
            sc.stop();
        }



    }
}
