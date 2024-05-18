import org.junit.jupiter.api.Test;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;

import static org.junit.jupiter.api.Assertions.*;

class MainTest {

    final String URL = "";

    @Test
    public void testMain() {
        Web3j client = Web3j.build( new HttpService(URL));
    }

}