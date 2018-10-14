package com.apps.solutions;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static java.sql.DriverManager.getConnection;

class AppSolution {
    protected static final ArrayList<LogContent> startedList = new ArrayList<>();
    protected static final ArrayList<LogContent> finishedList = new ArrayList<>();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = Logger.getLogger(AppSolution.class.getName());
    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS Alert (id VARCHAR(20), duration INTEGER, type VARCHAR(50), host VARCHAR(50), alert BOOLEAN)";
    private static final String PREPARED_STATEMENT = "INSERT INTO Alert (id, duration, type, host, alert)  Values (?, ?, ?, ?, ?)";
    private static final String STARTED = "STARTED";
    protected static Connection connection;

    public static void main(String[] args) throws IllegalAccessException, InstantiationException {
        logger.log(Level.INFO, "Starting AppSolution...");
        try (Stream<String> inputStream = Files.lines(Paths.get(args[0]))) {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
                   connection = getConnection("jdbc:mysql://localhost/appdb?" +
                    "user=root&password=root");
            connection.createStatement().execute(CREATE_TABLE);
            inputStream.forEachOrdered(AppSolution::splitFileIntoGroups);
            startedList.parallelStream().forEachOrdered(AppSolution::logBuilder);
            connection.close();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error opening File -" + e);
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error connecting to MySQL -" + e);
        } catch (ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Error MySQL class not found -" + e);
        }
        logger.log(Level.INFO, "Terminating AppSolution...");
    }

    /**
     * Takes a String line adds it to the Started or Finished list for further processing.
     *
     * @param line
     */
    protected static void splitFileIntoGroups(String line) {
        try {
            LogContent logContent = objectMapper.readValue(line, LogContent.class);
            if (logContent.getState().equals(STARTED)) {
                startedList.add(logContent);
            } else {
                finishedList.add(logContent);
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, "Error processing line -" + line);
        }
    }

    /**
     * Takes a LogEntry object and applies a filter to the finishedList to find its finishing log entry. The run duration is determined and alert flag set
     *
     * @param logContent
     */
    private static void logBuilder(LogContent logContent) {
        try {
            Stream<LogContent> result = finishedList
                    .stream()
                    .filter(item -> item.getId().equals(logContent.getId())
                            && (item.getHost().compareTo(logContent.getHost()) == item.getType().compareTo(logContent.getType())))
                    .limit(1);

            LogContent filteredLogContent = result.findFirst().get();
            long duration = Duration.between(logContent.getTimestamp().toInstant(), filteredLogContent.getTimestamp().toInstant()).toMillis();
            if (duration > 4) {
                saveAlerts(filteredLogContent, duration, true);
            } else {
                saveAlerts(filteredLogContent, duration, false);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error aggregating log content  -" + e);
        }
    }

    /**
     * Takes in LogEntry object along with duration and alert flag. This date is stored in DB
     *
     * @param logAlertContent
     * @param duration
     * @param isAlert
     */
    protected static Boolean saveAlerts(LogContent logAlertContent, long duration, Boolean isAlert) {
        try {
            PreparedStatement statement = connection.prepareStatement(PREPARED_STATEMENT);
            statement.setString(1, logAlertContent.getId());
            statement.setLong(2, duration);
            statement.setString(3, logAlertContent.getType());
            statement.setString(4, logAlertContent.getHost());
            statement.setBoolean(5, isAlert);
            return statement.executeUpdate() > 0;
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error saving alerts -" + e);
            return false;
        }
    }
}