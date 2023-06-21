package StoredProcedureTesting;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.*;

public class SPTesting {
    Connection con = null;
    Statement stmt;
    ResultSet rs;
    ResultSet rs1;
    ResultSet rs2;
    CallableStatement cstmt;

    @BeforeClass
    void setup() throws SQLException {
        con = DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels", "root", "root");
    }

    @AfterClass
    void teardown() throws SQLException {
        con.close();
    }

    @Test(priority = 1)
    void storedProcedureExists() throws SQLException {
        stmt = con.createStatement();
        rs = stmt.executeQuery("SHOW PROCEDURE STATUS WHERE  name = 'selectAllCustomers'");
        rs.next();

        Assert.assertEquals(rs.getString("Name"), "selectAllCustomers");
    }

    @Test(priority = 2)
    void selectAllCustomers() throws SQLException {
        cstmt = con.prepareCall("{call SelectAllCustomers()}");
        rs1 = cstmt.executeQuery();

        stmt = con.createStatement();
        rs2 = stmt.executeQuery("select * from customers");

        Assert.assertEquals((campareResultsSets(rs1, rs2)), true);
    }

    @Test(priority = 3)
    void selectAllCustomersByCity() throws SQLException {
        cstmt = con.prepareCall("{call selectAllCustomersByCity(?)}");
        cstmt.setString(1, "Singapore");
        rs1 = cstmt.executeQuery();

        stmt = con.createStatement();
        rs2 = stmt.executeQuery("SELECT * FROM customers WHERE city = 'Singapore'");

        Assert.assertEquals((campareResultsSets(rs1, rs2)), true);
    }

    @Test(priority = 4)
    void selectAllCustomersByCityandPin() throws SQLException {
        cstmt = con.prepareCall("{call selectAllCustomersByCityandPin(?,?)}");
        cstmt.setString(1, "Singapore");
        cstmt.setString(2, "079903");
        rs1 = cstmt.executeQuery();

        stmt = con.createStatement();
        rs2 = stmt.executeQuery("SELECT * FROM customers WHERE city = 'Singapore' and postalCode='079903'");

        Assert.assertEquals((campareResultsSets(rs1, rs2)), true);
    }

    public boolean campareResultsSets(ResultSet rs1, ResultSet rs2) throws SQLException {
        while (rs1.next()) {
            rs2.next();
            int count = rs1.getMetaData().getColumnCount();
            for (int i = 1; i <= count; i++) {
                if (!StringUtils.equals(rs1.getString(i), rs2.getString(i))) {
                    return false;
                }
            }

        }
        return true;
    }

}
