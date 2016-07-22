package com.jasongj.jta.util;

import java.util.Enumeration;
import java.util.Hashtable;

import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.apache.naming.ResourceRef;
import org.apache.naming.factory.Constants;

import com.atomikos.beans.PropertyException;
import com.atomikos.beans.PropertyUtils;
import com.atomikos.jdbc.AbstractDataSourceBean;
import com.atomikos.jdbc.AtomikosDataSourceBean;
import com.atomikos.jdbc.AtomikosSQLException;
import com.atomikos.jms.AtomikosConnectionFactoryBean;
import com.atomikos.util.IntraVmObjectRegistry;

public class EnhancedTomcatAtomikosBeanFactory implements ObjectFactory {

  public Object getObjectInstance(Object obj, Name name, Context nameCtx,
      Hashtable<?, ?> environment) throws NamingException {
    if (obj instanceof ResourceRef) {
      try {

        Reference ref = (Reference) obj;
        findAndCloseUniqueResource(ref);

        String beanClassName = ref.getClassName();
        Class<?> beanClass = null;
        ClassLoader tcl = Thread.currentThread().getContextClassLoader();
        if (tcl != null) {
          try {
            beanClass = tcl.loadClass(beanClassName);
          }
          catch (ClassNotFoundException e) {
          }
        }
        else {
          try {
            beanClass = Class.forName(beanClassName);
          }
          catch (ClassNotFoundException e) {
            e.printStackTrace();
          }
        }
        if (beanClass == null) {
          throw new NamingException("Class not found: " + beanClassName);
        }
        if (AtomikosDataSourceBean.class.isAssignableFrom(beanClass)) {
          return createDataSourceBean(ref, beanClass);
        }
        else if (AtomikosConnectionFactoryBean.class
            .isAssignableFrom(beanClass)) {
          return createConnectionFactoryBean(ref, beanClass);
        }
        else {
          throw new NamingException(
              "Class is neither an AtomikosDataSourceBean nor an AtomikosConnectionFactoryBean: "
                  + beanClassName);
        }

      }
      catch (Exception ex) {
        throw (NamingException) new NamingException(
            "error creating AtomikosDataSourceBean").initCause(ex);
      }

    }
    else {
      return null;
    }
  }

  /**
   * create a DataSourceBean for a JMS datasource
   * 
   * @param ref
   * @param beanClass
   * @return
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws PropertyException
   * @throws AtomikosSQLException
   * @throws JMSException
   */
  private Object createConnectionFactoryBean(Reference ref, Class<?> beanClass)
      throws InstantiationException, IllegalAccessException,
      PropertyException, JMSException {
    AtomikosConnectionFactoryBean bean =
        (AtomikosConnectionFactoryBean) beanClass.newInstance();

    Enumeration<RefAddr> en = ref.getAll();
    while (en.hasMoreElements()) {
      RefAddr ra = en.nextElement();
      String propName = ra.getType();

      if (ignoredProperty(propName)) {
        continue;
      }

      String value = (String) ra.getContent();

      PropertyUtils.setProperty(bean, propName, value);
    }

    bean.init();
    return bean;
  }

  /**
   * create a DataSourceBean for a JDBC datasource
   * 
   * @param ref
   * @param beanClass
   * @return
   * @throws InstantiationException
   * @throws IllegalAccessException
   * @throws PropertyException
   * @throws AtomikosSQLException
   */
  private Object createDataSourceBean(Reference ref, Class<?> beanClass)
      throws InstantiationException, IllegalAccessException,
      PropertyException, AtomikosSQLException {
    AtomikosDataSourceBean bean =
        (AtomikosDataSourceBean) beanClass.newInstance();

    Enumeration<RefAddr> en = ref.getAll();
    while (en.hasMoreElements()) {
      RefAddr ra = en.nextElement();
      String propName = ra.getType();

      if (ignoredProperty(propName)) {
        continue;
      }

      String value = (String) ra.getContent();
      PropertyUtils.setProperty(bean, propName, value);
    }

    bean.init();
    return bean;
  }

  private boolean ignoredProperty(String propName) {
    return propName.equals(Constants.FACTORY) 
        || propName.equals("singleton")
        || propName.equals("description") 
        || propName.equals("scope")
        || propName.equals("auth");
  }

  /**
   * Check and close if any AtomikosConnectionFactoryBean and
   * AbstractDataSourceBean exists
   * @param ref
   */
  private void findAndCloseUniqueResource(Reference ref) {
    Enumeration<RefAddr> all = ref.getAll();
    while (all.hasMoreElements()) {
      RefAddr refAddrElement = all.nextElement();
      if (refAddrElement.getType().equals("uniqueResourceName")) {
        closeIfExist(refAddrElement.getContent().toString());
      }
    }
  }

  /**
   * Close the existing AtomikosConnectionFactoryBean and
   * AbstractDataSourceBean.
   * @param aName
   */
  private void closeIfExist(String aName) {
    try {
      Object o = IntraVmObjectRegistry.getResource(aName);
      if (o != null) {
        try {
          if (o instanceof AtomikosConnectionFactoryBean) {
            AtomikosConnectionFactoryBean o1 =
                (AtomikosConnectionFactoryBean) o;
            o1.close();
          }
          else if (o instanceof AbstractDataSourceBean) {
            AbstractDataSourceBean o1 = (AbstractDataSourceBean) o;
            o1.close();
          }
        }
        catch (Exception se) {
          se.printStackTrace();
        }
        IntraVmObjectRegistry.removeResource(aName);
      }
    }
    catch (Exception e) {
    }
  }

}