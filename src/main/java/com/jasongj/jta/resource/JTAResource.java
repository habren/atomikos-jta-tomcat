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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/jta")
public class JTAResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(JTAResource.class);

  @GET
  public String test(@PathParam(value = "commit") boolean isCommit)
      throws NamingException, SQLException, NotSupportedException, SystemException {
    UserTransaction userTransaction = null;
    try {
      Context context = new InitialContext();
      userTransaction = (UserTransaction) context.lookup("java:comp/UserTransaction");
      userTransaction.setTransactionTimeout(600);
      
      userTransaction.begin();
      
      DataSource dataSource1 = (DataSource) context.lookup("java:comp/env/jdbc/1");
      Connection xaConnection1 = dataSource1.getConnection();
      
      DataSource dataSource2 = (DataSource) context.lookup("java:comp/env/jdbc/2");
      Connection xaConnection2 = dataSource2.getConnection();
      LOGGER.info("Connection autocommit : {}", xaConnection1.getAutoCommit());

      Statement st1 = xaConnection1.createStatement();
      Statement st2 = xaConnection2.createStatement();
      LOGGER.info("Connection autocommit after created statement: {}", xaConnection1.getAutoCommit());
      

      st1.execute("update casp.test set qtime=current_timestamp, value = 1");
      st2.execute("update casp.test set qtime=current_timestamp, value = 2");
      LOGGER.info("Autocommit after execution : ", xaConnection1.getAutoCommit());

      userTransaction.commit();
      LOGGER.info("Autocommit after commit: ",  xaConnection1.getAutoCommit());
      return "commit";

    } catch (Exception ex) {
      if (userTransaction != null) {
        userTransaction.rollback();
      }
      LOGGER.info(ex.toString());
      throw new WebApplicationException("failed", ex);
    }
  }

}
