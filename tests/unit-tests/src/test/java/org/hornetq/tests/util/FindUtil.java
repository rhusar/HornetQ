package org.hornetq.tests.util;

import java.io.File;
import java.net.URL;

/**
 * Find files within test
 *
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class FindUtil
{
   public static File computeTestDataRoot(Class anyTestClass)
   {
      final String clsUri = anyTestClass.getName().replace('.', '/') + ".class";
      final URL url = anyTestClass.getClassLoader().getResource(clsUri);
      final String clsPath = url.getPath();
      final File root = new File(clsPath.substring(0, clsPath.length() - clsUri.length()));
      return root;
   }

}
