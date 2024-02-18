package com.ccsu.resource.metadata;

import com.ccsu.MetadataService;
import com.ccsu.pojo.DatasourceConfig;
import com.ccsu.meta.data.MetaIdentifier;
import com.ccsu.meta.data.MetaPath;
import lombok.Data;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/datasource")
public class DataSourceResource {

    private final MetadataService metadataService;

    @Inject
    public DataSourceResource(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    @POST
    @Path("/register/catalog")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public String registerCatalog(CatalogRegisterRequest request) {
        metadataService.registerCatalog(new MetaIdentifier(request.clusterId, MetaPath.buildCatalogPath(request.catalogName)), request.datasourceConfig);
        return "Success";
    }

    @Data
    public static class CatalogRegisterRequest {
        private String clusterId;
        private String catalogName;
        private DatasourceConfig datasourceConfig;
    }
}
