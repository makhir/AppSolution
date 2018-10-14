package com.apps.solutions;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.sql.DriverManager.getConnection;
import static org.junit.jupiter.api.Assertions.*;

class AppSolutionTest {
    private static final Logger logger = Logger.getLogger(AppSolutionTest.class.getName());
    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS Alert (id VARCHAR(20), duration INTEGER, type VARCHAR(50), host VARCHAR(50), alert BOOLEAN)";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static LogContent validObject;
    private static LogContent validObjectWithAdditionalFields;

    @BeforeEach
    void setup() throws IllegalAccessException, InstantiationException {
        logger.log(Level.INFO, "Begin Testing AppSolution...");
        try {

            Class.forName("com.mysql.jdbc.Driver").newInstance();
            AppSolution.connection = DriverManager.getConnection("jdbc:mysql://localhost/appdb?"+"user=root&password=root");

            AppSolution.connection.createStatement().execute(CREATE_TABLE);
            validObject = objectMapper.readValue("{\"id\":\"scsmbstgrc\", \"state\":\"STARTED\", \"timestamp\":1491377495218}", LogContent.class);
            validObjectWithAdditionalFields = objectMapper.readValue("{\"id\":\"scsmbstgra\", \"state\":\"STARTED\", \"type\":\"APPLICATION_LOG\", \"host\":\"12345\", \"timestamp\":1491377495212}", LogContent.class);
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error connecting to MySQL -" + e);
        } catch (ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Error MySQL class not found -" + e);
        } catch (JsonParseException e) {
            logger.log(Level.SEVERE, "Error Parsing Test Json String -" + e);
        } catch (JsonMappingException e) {
            logger.log(Level.SEVERE, "Error Mapping Test Json to Pojo -" + e);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error class used in tests not found -" + e);
        }
    }

    @BeforeEach
    void resetLists() {
        AppSolution.startedList.clear();
        AppSolution.finishedList.clear();
    }

    @AfterEach
    void closeConnection() {
        try {
            AppSolution.connection.close();
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error closing connection -" + e);
        }
    }

    @Test
    void splitFileIntoGroupsTest_ValidJsonMissingRequiredField() {
        String testData = "{\"state\":\"STARTED\", \"timestamp\":1491377495218}";
        assertEquals(0, AppSolution.startedList.size());
        assertEquals(0, AppSolution.finishedList.size());

        AppSolution.splitFileIntoGroups(testData);

        assertEquals(0, AppSolution.startedList.size());
        assertEquals(0, AppSolution.finishedList.size());
    }

    @Test
    void splitFileIntoGroupsTest_InvalidJson() {
        String testData = "invalid json object";
        assertEquals(0, AppSolution.startedList.size());
        assertEquals(0, AppSolution.finishedList.size());

        AppSolution.splitFileIntoGroups(testData);

        assertEquals(0, AppSolution.startedList.size());
        assertEquals(0, AppSolution.finishedList.size());
    }

    @Test
    void splitFileIntoGroupsTest_ValidStartedJson() {
        String testData = "{\"id\":\"scsmbstgrc\", \"state\":\"STARTED\", \"timestamp\":1491377495218}";
        assertEquals(0, AppSolution.startedList.size());
        assertEquals(0, AppSolution.finishedList.size());

        AppSolution.splitFileIntoGroups(testData);

        assertEquals(1, AppSolution.startedList.size());
        assertEquals(0, AppSolution.finishedList.size());
    }

    @Test
    void splitFileIntoGroupsTest_ValidFinishedJson() {
        String testData = "{\"id\":\"scsmbstgrc\", \"state\":\"FINISHED\", \"timestamp\":1491377495218}";
        assertEquals(0, AppSolution.startedList.size());
        assertEquals(0, AppSolution.finishedList.size());

        AppSolution.splitFileIntoGroups(testData);

        assertEquals(0, AppSolution.startedList.size());
        assertEquals(1, AppSolution.finishedList.size());
    }

    @Test
    void splitFileIntoGroupsTest_ValidStartedJsonWithAdditionalFields() {
        String testData = "{\"id\":\"scsmbstgra\", \"state\":\"STARTED\", \"type\":\"APPLICATION_LOG\", \"host\":\"12345\", \"timestamp\":1491377495212}";
        assertEquals(0, AppSolution.startedList.size());
        assertEquals(0, AppSolution.finishedList.size());

        AppSolution.splitFileIntoGroups(testData);

        assertEquals(1, AppSolution.startedList.size());
        assertEquals(0, AppSolution.finishedList.size());
    }

    @Test
    void splitFileIntoGroupsTest_ValidFinishedJsonWithAdditionalFields() {
        String testData = "{\"id\":\"scsmbstgra\", \"state\":\"Finished\", \"type\":\"APPLICATION_LOG\", \"host\":\"12345\", \"timestamp\":1491377495212}";
        assertEquals(0, AppSolution.startedList.size());
        assertEquals(0, AppSolution.finishedList.size());

        AppSolution.splitFileIntoGroups(testData);

        assertEquals(0, AppSolution.startedList.size());
        assertEquals(1, AppSolution.finishedList.size());
    }

    @Test
    void saveAlertsTest_ValidJson() {
        assertTrue(AppSolution.saveAlerts(validObject, 1, true));
        assertTrue(AppSolution.saveAlerts(validObject, 1, false));

        assertTrue(AppSolution.saveAlerts(validObject, 1000000, true));
        assertTrue(AppSolution.saveAlerts(validObject, 1000000, false));
    }

    @Test
    void saveAlertsTest_ValidJsonWithAdditionalFields() {
        assertTrue(AppSolution.saveAlerts(validObjectWithAdditionalFields, 1, true));
        assertTrue(AppSolution.saveAlerts(validObjectWithAdditionalFields, 1, false));

        assertTrue(AppSolution.saveAlerts(validObjectWithAdditionalFields, 1000000, true));
        assertTrue(AppSolution.saveAlerts(validObjectWithAdditionalFields, 1000000, false));
    }

    @Test
    void saveAlertsTest_InvalidJson() {
        assertFalse(AppSolution.saveAlerts(null, 1, true));
        assertFalse(AppSolution.saveAlerts(null, 1, false));

        assertFalse(AppSolution.saveAlerts(null, 1000000, true));
        assertFalse(AppSolution.saveAlerts(null, 1000000, false));
    }

    @Test
    void mainTest_FullRunThrough() throws InstantiationException, IllegalAccessException {
        assertEquals(0, AppSolution.startedList.size());
        assertEquals(0, AppSolution.finishedList.size());

        String[] inputArgs = {"src/test/resources/testData.txt"};
        AppSolution.main(inputArgs);

        assertEquals(3, AppSolution.startedList.size());
        assertEquals(3, AppSolution.finishedList.size());

        try {
            Statement statement = AppSolution.connection.createStatement();
            assertEquals(3, statement.executeQuery("SELECT count(*) FROM Alert AS count").getInt("count"));
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error executing query -" + e);
        }
    }
}