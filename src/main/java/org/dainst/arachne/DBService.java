/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dainst.arachne;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Reimar Grabowski
 */
public class DBService {

    private Connection connection = null;
    
    private final String dbUrl;
    
    private final String dbName;
    
    private final String userName;
    
    private final String password;

    public DBService() throws ClassNotFoundException {
        Properties props = new Properties();
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("dbConfig.properties");
        if (inputStream != null) {
            try {
                props.load(inputStream);
            } catch (IOException ex) {
                Logger.getLogger(ESService.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    
        dbUrl = props.getProperty("url");
        dbName = props.getProperty("name");
        userName = props.getProperty("user");
        password = props.getProperty("password");
        
        Class.forName("com.mysql.jdbc.Driver");
    }
    
    public void close() throws SQLException {
        if (connection != null) connection.close();
    }

    private Connection getConnection() {
        if (connection != null) {
            return connection;
        } else {
            try {
                connection = DriverManager.getConnection("jdbc:mysql://" + dbUrl + "/" + dbName + "?"
                        + "useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&"
                        + "user=" + userName + "&password=" + password);
            } catch (SQLException ex) {
                Logger.getLogger(DBService.class.getName()).log(Level.SEVERE, null, ex);
            }
            return connection;
        }
    }

    public Long queryDBForLong(final String sql) throws SQLException {
        ResultSet resultSet = null;
        Statement statement = null;
        Long result = null;
        try {
            statement = getConnection().createStatement();
            resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                result = resultSet.getLong("ArachneEntityID");
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }
        }
        return result;
    }

    public String queryDBForString(final String sql) throws SQLException {
        ResultSet resultSet = null;
        Statement statement = null;
        String result = null;
        try {
            statement = getConnection().createStatement();
            resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
                result = resultSet.getString("DateinameMarbilder");
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }

            if (statement != null) {
                statement.close();
            }
        }
        return result;
    }

}
