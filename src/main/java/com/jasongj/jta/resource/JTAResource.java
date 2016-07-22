package com.jasongj.jta.resource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;


@Path("/jta")
public class JTAResource {

  @GET
  public String test(@PathParam(value = "commit") boolean isCommit)
      throws NamingException, SQLException, NotSupportedException, SystemException {
    UserTransaction userTransaction = null;
    try {
      Context context = new InitialContext();
      userTransaction = (UserTransaction) context.lookup("java:comp/UserTransaction");
      userTransaction.setTransactionTimeout(600);
//      userTransaction = com.arjuna.ats.jta.UserTransaction.userTransaction();
      
      userTransaction.begin();

      
      DataSource dataSource229 = (DataSource) context.lookup("java:comp/env/jdbc/229");
      Connection xaConnection229 = dataSource229.getConnection();
//      XAResource xaResource229 = xaConnection229.getXAResource();
      System.out.println("Autocommit : " + xaConnection229.getAutoCommit());
      
      DataSource dataSource94 = (DataSource) context.lookup("java:comp/env/jdbc/94");
      Connection xaConnection94 = dataSource94.getConnection();
//      XAResource xaResource94 = xaConnection94.getXAResource();

//      Transaction transaction = userTransaction.getTransaction();
//      transaction.enlistResource(xaResource94);
//      transaction.enlistResource(xaResource229);
      
      
//      Connection connection94 = xaConnection94.getConnection();
      Statement st94 = xaConnection94.createStatement();
      
//      Connection connection229 = xaConnection229.getConnection();
      Statement st229 = xaConnection229.createStatement();
      System.out.println("Autocommit : " + xaConnection229.getAutoCommit());
      
      st94.execute("create table casp.test94(qtime timestamptz, value integer)");

      st229.execute("create table casp.test229(qtime timestamptz, value integer)");
      System.out.println("Autocommit : " + xaConnection229.getAutoCommit());
      


//      userTransaction.rollback();
      userTransaction.commit();
      System.out.println("Autocommit : " + xaConnection229.getAutoCommit());
      return "commit";

    } catch (Exception ex) {
      if (userTransaction != null) {
        userTransaction.rollback();
      }
      ex.printStackTrace();
      throw new WebApplicationException("failed", ex);
    }
  }

}
