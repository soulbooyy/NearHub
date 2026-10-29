package com.hmdp.cache;

import com.hmdp.config.WebExceptionAdvice;
import com.hmdp.controller.ShopController;
import com.hmdp.service.IShopService;
import com.hmdp.service.CacheRebuildBusyException;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class CacheRebuildWebTest {
    @Test void contentionReturns503UsingExistingResultContract() throws Exception {
        ShopController controller = new ShopController();
        controller.shopService = mock(IShopService.class);
        when(controller.shopService.queryById(1L)).thenThrow(new CacheRebuildBusyException("busy"));
        MockMvc mvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new WebExceptionAdvice()).build();
        mvc.perform(get("/shop/1")).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.errorMsg").value("商铺信息正在加载，请稍后重试"));
    }
}
