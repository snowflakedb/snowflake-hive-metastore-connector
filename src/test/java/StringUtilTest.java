import com.snowflake.core.util.StringUtil;
import com.snowflake.core.util.StringUtil.SensitiveString;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the string utility class
 */
public class StringUtilTest
{
  /**
   * Tests the correctness of the format helper method
   * @throws Exception
   */
  @Test
  public void formatTest() throws Exception
  {
    Map<String, String> map = new HashMap<>();

    // An empty map means no substitutions, which should yield the same string
    String strNoOp = StringUtil.format("{one} + {two} = {three}", map);
    assertEquals("{one} + {two} = {three}", strNoOp);

    // Test a simple case with simple substitutions
    map.put("one", "1");
    map.put("two", "2");
    map.put("three", "3");
    String strBasic = StringUtil.format("{one} + {two} = {three}", map);
    assertEquals("1 + 2 = 3", strBasic);

    // Test an advanced case
    map.put("three", "{one}"); // Substitutions should not be replaced again
    map.put("11", "oneone"); // Substitutions should not be replaced again
    map.put("four", "4"); // Keys that aren't referenced should be skipped
    map.put("", "twotwo"); // Empty strings as keys should be skipped

    String strAdvanced = StringUtil.format("{1{one}} + {two} = {three}", map);

    assertEquals("{11} + 2 = {one}", strAdvanced);
  }

  /**
   * Basic test for the correctness of the SensitiveString class
   * @throws Exception
   */
  @Test
  public void sensitiveStringTest() throws Exception
  {
    Map<String, String> map = new HashMap<>();
    map.put("one", "1");
    map.put("two", "2");
    map.put("three", "3");

    SensitiveString str = new SensitiveString("{one} + {two} = {three}", map);
    assertEquals("{one} + {two} = {three}", str.toString());
    assertEquals("{one} + {two} = {three}", "" + str);
    assertEquals("1 + 2 = 3", str.toStringWithSensitiveValues());
  }
}
