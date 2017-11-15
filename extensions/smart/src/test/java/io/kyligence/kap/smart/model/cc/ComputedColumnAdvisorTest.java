package io.kyligence.kap.smart.model.cc;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class ComputedColumnAdvisorTest {

    @Test
    public void sumCCTest() {
        String sql = "select SUM(CASE when MARA_MEDICARE_RISK_SCORE>1 then 1 else 0 end), PAYER as a from Z_PROVDASH_UM_ED group by PAYER";
        
        List<String> suggestion = new ComputedColumnAdvisor().suggestCandidate(sql);
        
        Assert.assertNotNull(suggestion);
        Assert.assertEquals(1, suggestion.size());
        Assert.assertEquals("CASE WHEN \"MARA_MEDICARE_RISK_SCORE\" > 1 THEN 1 ELSE 0 END", suggestion.get(0));
    }
    
    @Test
    public void arrayItemCCTest() {
        String sql = "select ARRAY['123', '222'][1],  PAYER from Z_PROVDASH_UM_ED";
        
        List<String> suggestion = new ComputedColumnAdvisor().suggestCandidate(sql);
        
        Assert.assertNotNull(suggestion);
        Assert.assertEquals(1, suggestion.size());
        Assert.assertEquals("ARRAY['123', '222'][1]", suggestion.get(0));
    }
}
