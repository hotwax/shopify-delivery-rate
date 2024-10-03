/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package co.hotwax.shopify

import groovy.transform.CompileStatic
import org.moqui.entity.EntityCondition
import org.moqui.entity.EntityList
import org.moqui.entity.EntityValue
import org.moqui.impl.context.ContextJavaUtil
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.moqui.impl.context.ExecutionContextImpl
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.commons.io.IOUtils

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import javax.servlet.*
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

@CompileStatic
class ShopifyRequestFilter implements Filter {

    protected static final Logger logger = LoggerFactory.getLogger(ShopifyRequestFilter.class)
    protected FilterConfig filterConfig = null

    ShopifyRequestFilter() { super() }

    @Override
    void init(FilterConfig filterConfig) {
        this.filterConfig = filterConfig
    }

    @Override
    void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) {
        if (!(req instanceof HttpServletRequest) || !(resp instanceof HttpServletResponse)) {
            chain.doFilter(req, resp); return
        }

        HttpServletRequest request = (HttpServletRequest) req
        HttpServletResponse response = (HttpServletResponse) resp

        ServletContext servletContext = req.getServletContext()

        ExecutionContextFactoryImpl ecfi = (ExecutionContextFactoryImpl) servletContext.getAttribute("executionContextFactory")
        // check for and cleanly handle when executionContextFactory is not in place in ServletContext attr
        if (ecfi == null) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "System is initializing, try again soon.")
            return
        }

        try {
            // Verify the incoming shopify request
            verifyIncomingRequest(request, response, ecfi.getEci())
            chain.doFilter(req, resp)
        } catch(Throwable t) {
            logger.error("Error occurred in Shopify request verification ${request.getPathInfo()}", t)
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error in Shopify request ${request.getPathInfo()} verification: ${t.toString()}")
        }
    }

    @Override
    void destroy() {
        // Your implementa tion here }
    }

    void verifyIncomingRequest(HttpServletRequest request, HttpServletResponse response, ExecutionContextImpl ec) {

        String hmac = request.getHeader("X-Shopify-Hmac-SHA256")
        String shopDomain = request.getHeader("X-Shopify-Shop-Domain")

        String requestBody = IOUtils.toString(request.getReader());
        if (requestBody.length() == 0) {
            logger.warn("The request body for shopify request is empty for Shopify ${shopDomain}, cannot verify hmac")
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "The Request Body is empty for Shopify request")
            return
        }
        //request.setAttribute("payload", ContextJavaUtil.jacksonMapper.readValue(requestBody, Map.class))
        // Parse request body JSON and put each entry as a request attribute
        Map<String, Object> payloadMap = ContextJavaUtil.jacksonMapper.readValue(requestBody, Map.class)
        payloadMap.each { key, value ->
            request.setAttribute(key, value)
        }
        
        EntityList systemMessageRemoteList = ec.entityFacade.find("moqui.service.message.SystemMessageRemote")
                .condition("sendUrl", EntityCondition.ComparisonOperator.LIKE, "%"+shopDomain+"%")
                .condition("sendSharedSecret", EntityCondition.ComparisonOperator.NOT_EQUAL, null)
                .useCache(true)
                .disableAuthz().list()
        /*EntityList shopifyShopConfigs = ec.entityFacade.find("co.hotwax.shopify.ShopifyShopAndConfig")
                .condition("myshopifyDomain", EntityCondition.ComparisonOperator.LIKE, "%"+shopDomain+"%")
                .condition("sharedSecret", EntityCondition.ComparisonOperator.NOT_EQUAL, null)
                .disableAuthz().list()
        //We should use ShopifyShopAndConfig but there is an issue with encrypted field read
        */
        for (EntityValue systemMessageRemote in systemMessageRemoteList) {
            boolean isHmacVerified = verifyHmac(requestBody, hmac, systemMessageRemote.getString("sendSharedSecret"), 'Base64');

            // If the hmac matched with the calculatedHmac, break the loop and return
            if (isHmacVerified) {
                request.setAttribute("systemMessageRemoteId", systemMessageRemote.systemMessageRemoteId)
                return;
            }
        }
        logger.warn("The shopify ${request.getPathInfo()} HMAC header did not match with the computed HMAC for Shopify ${shopDomain}")
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "HMAC verification failed for Shopify ${shopDomain} ${request.getPathInfo()} request")
    }
    private boolean verifyHmac(String message, String hmac, String sharedSecret, String digest) {
        Mac hmacSha256 = Mac.getInstance("HmacSHA256")
        hmacSha256.init(new SecretKeySpec(sharedSecret.getBytes("UTF-8"), "HmacSHA256"))
        byte[] bytes = hmacSha256.doFinal(message.getBytes("UTF-8"));
        String calculatedHmac = "";
        if ("Base64".equals(digest)) {
            calculatedHmac = Base64.encoder.encodeToString(bytes)
        } else if ("Hex".equals(digest)) {
            calculatedHmac = org.apache.commons.codec.binary.Hex.encodeHexString(bytes)
        }
        //TODO: Fix me equals is not safe
        return calculatedHmac.equals(hmac)
    }
}