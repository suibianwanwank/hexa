package com.ccsu.resource.metadata;

import com.ccsu.datasource.MetadataRegister;
import com.ccsu.datasource.api.pojo.DatasourceConfig;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/datasource")
public class DataSourceResource {

    private final MetadataRegister metadataRegister;

    @Inject
    public DataSourceResource(MetadataRegister metadataRegister) {
        this.metadataRegister = metadataRegister;
    }

    @POST
    @Path("/register/catalog")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    public String registerCatalog(DatasourceConfig datasourceConfig) {
        metadataRegister.addMetaDataCollector(datasourceConfig);
        return "Success";
    }
}
