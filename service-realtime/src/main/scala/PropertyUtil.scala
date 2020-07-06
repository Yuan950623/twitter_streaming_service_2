import java.io.InputStreamReader
import java.util.Properties

object PropertyUtil {

  def readProp(propName: String): Properties = {
    val property = new Properties()
    property.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propName), "UTF-8"))
    property
  }
}
