package uk.ac.ed.inf.ilptutorialrestservice.controller;

import com.google.gson.Gson;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.inf.ilptutorialrestservice.dto.Restaurant;
import uk.ac.ed.inf.ilptutorialrestservice.dto.Tuple;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Objects;

/**
 * the ILP Tutorial service which provides suppliers, orders and other useful things
 */
@RestController
public class CoreRestController {

    /**
     * get a buffered reader for a resource
     *
     * @param jsonResource the JSON resource this reader is required for
     * @return the buffered reader
     */
    private java.io.BufferedReader getBufferedReaderForResource(String jsonResource) {
        return new BufferedReader(new InputStreamReader(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream(jsonResource))));
    }

    /**
     * returns the restaurants in the system
     *
     * @return array of suppliers
     */
    @GetMapping("/restaurants")
    public Restaurant[] restaurants() {
        return new Gson().fromJson(getBufferedReaderForResource("json/restaurants.json"), Restaurant[].class);
    }

    /**
     * simple test method to test the service's availability
     *
     * @param input an optional input which will be echoed
     * @return the echo
     */
    @GetMapping(value = {"/testPath/{input}", "/testPath"})
    public String test(@PathVariable(required = false) String input) {
        return String.format("Hello from the ILP-Tutorial-REST-Service. Your provided value was: %s", input == null ? "not provided" : input);
    }

    /**
     * GET with HTML result
     * @return
     */
    @RequestMapping(value = "/test", method = RequestMethod.GET, produces = MediaType.TEXT_HTML_VALUE)
    @ResponseBody
    public String testHtml() {
        return """
                <html>
                <header><title>ILP Tutorial REST Server</title></header>
                <body>
                <h1>Hello from the ILP Tutorial REST Server
                </h1></body>
                </html>""";
    }

    /**
     * POST with a JSON data structure in the request body
     * @param postAttribute
     * @return
     */
    @PostMapping(value = "/testPostBody",  consumes = {"*/*"})
    public String testPost(@RequestBody Tuple postAttribute) {
        return "You posted: " + postAttribute.toString();
    }

    /**
     * POST with request parameters
     * @param item1
     * @param item2
     * @return
     */
    @PostMapping("/testPostPath")
    public String testPost(@RequestParam("item1") String item1, @RequestParam("item2") String item2) {
        var postAttribute = new Tuple(item1, item2);
        return "You posted: " + postAttribute.toString();
    }
}
