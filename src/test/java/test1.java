import org.junit.*;
import static org.junit.Assert.*;
import hw2.exmaple.org.HW2;


public class test1 {
    private HW2 hw2;

    @Before
    public void initTest() {
        hw2 = new HW2();
    }

    @Test
    public void tetsArgs() throws Exception {
        assertNotNull(hw2);
    }


}
