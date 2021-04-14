package com.grakra;

import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;

/**
 * Created by grakra on 18-9-4.
 */
public class TestMethodCapture implements IInvokedMethodListener {
  private static ThreadLocal<ITestNGMethod> currentMethods = new ThreadLocal<ITestNGMethod>();
  private static ThreadLocal<ITestResult> currentResults = new ThreadLocal<ITestResult>();

  @Override
  public void beforeInvocation(IInvokedMethod method, ITestResult testResult) {
    currentMethods.set(method.getTestMethod());
    currentResults.set(testResult);
  }

  @Override
  public void afterInvocation(IInvokedMethod method, ITestResult testResult) {
    currentMethods.remove();
    currentResults.remove();
  }

  public static ITestNGMethod getTestMethod() {
    return currentMethods.get();
  }

  public static ITestResult getTestResult() {
    return currentResults.get();
  }
}
