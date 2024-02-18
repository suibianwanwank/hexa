package com.ccsu.schedule;

import com.ccsu.error.CommonErrorCode;
import com.ccsu.error.CommonException;
import com.ccsu.event.Event;
import com.ccsu.event.EventListener;
import com.ccsu.grpc.client.MetaBridgeClient;
import com.facebook.airlift.log.Logger;
import com.google.inject.Inject;

public class CollectorEventListener implements EventListener<CollectInfo> {

    private static final Logger LOGGER = Logger.get(CollectorEventListener.class);

    private final MetaBridgeClient bridgeClient;

    @Inject
    public CollectorEventListener(MetaBridgeClient bridgeClient) {
        this.bridgeClient = bridgeClient;
    }

    @Override
    public void onEvent(Event<CollectInfo> event) throws CommonException {
        CollectInfo info = event.getData();
        switch (info.getCollectType()) {
            case COLLECT_TABLE_NAME_IN_CATALOG: {
                bridgeClient.syncCollectAllItemInCatalog(info.getDatasourceConfig());
                LOGGER.info("finish table name collect" + info.getCollectScope().getCatalogName());
                break;
            }
            case COLLECT_TABLE_DETAIL: {
                bridgeClient.syncCollectTableDetail(info.getDatasourceConfig(), info.getCollectScope().getSchemaName(), info.getCollectScope().getTableName());
                break;
            }
            default: {
                throw new CommonException(CommonErrorCode.META_COLLECT_ERROR,
                        String.format("Not support collect type: %s", info.getCollectType()));
            }
        }
    }
}
