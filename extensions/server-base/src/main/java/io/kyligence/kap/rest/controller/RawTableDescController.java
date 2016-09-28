package io.kyligence.kap.rest.controller;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.cube.raw.RawTableDesc;
import io.kyligence.kap.cube.raw.RawTableInstance;
import io.kyligence.kap.rest.service.RawTableService;

@Controller
@RequestMapping(value = "/raw_desc")
public class RawTableDescController {

    @Autowired
    private RawTableService rawService;

    /**
     * Get detail information of the "Cube ID"
     * return CubeDesc instead of CubeDesc[]
     *
     * @param rawName
     *            Cube ID
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{rawName}", method = { RequestMethod.GET })
    @ResponseBody
    public RawTableDesc getDesc(@PathVariable String rawName) {
        RawTableInstance rawInstance = rawService.getRawTableManager().getRawTableInstance(rawName);
        if (rawInstance == null) {
            return null;
        }
        RawTableDesc cSchema = rawInstance.getRawTableDesc();
        if (cSchema != null) {
            return cSchema;
        } else {
            return null;
        }
    }

    public void setRawService(RawTableService rawService) {
        this.rawService = rawService;
    }

}
