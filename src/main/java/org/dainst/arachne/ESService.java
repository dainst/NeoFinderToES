package org.dainst.arachne;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 *
 * @author Reimar Grabowski
 */
public class ESService {

    private final TransportClient client;

    private boolean clusterAvailable;
    
    public ESService(final InetAddress address, final String clusterName) {
        final InetSocketAddress esAddress = new InetSocketAddress(address, 9300);
        client = TransportClient.builder()
                .settings(Settings.builder()
                        .put("cluster.name", clusterName)
                        .build()).build()
                .addTransportAddress(new InetSocketTransportAddress(esAddress));
        
        // test if we are connected to an es cluster
        try {
            ClusterStatsResponse stats = client.admin().cluster().prepareClusterStats().get("1s");
            clusterAvailable = true;
        } catch (NoNodeAvailableException e) {
            System.out.println("Could not connect to elasticsearch cluster");
            clusterAvailable = false;
        }       
    }
    
    public void close() {
        client.close();
    }

    public boolean createIndex(final String indexName) {
        try {
            final CreateIndexResponse createResponse = client.admin().indices().prepareCreate(indexName).execute().actionGet();
            if (!createResponse.isAcknowledged()) {
                Logger.getLogger(ESService.class.getName()).log(Level.SEVERE, "Failed to create index ''{0}''", indexName);
                return false;
            }
        } catch (ElasticsearchException e) {
            return false;
        }
        return true;
    }

    public boolean deleteIndex(final String indexName) {
        try {
            final DeleteIndexResponse delete = client.admin().indices().prepareDelete(indexName).execute().actionGet();
            if (!delete.isAcknowledged()) {
                Logger.getLogger(ESService.class.getName()).log(Level.SEVERE, "Index {0} was not deleted.", indexName);
                return false;
            }
        } catch (ElasticsearchException e) {
            return false;
        }
        return true;
    }
    
    public boolean indexExists(final String indexName) {
        try {
            final IndicesExistsResponse existsResponse = client.admin().indices().prepareExists(indexName).execute().actionGet();
            return existsResponse.isExists();
        } catch (ElasticsearchException e) {
            return false;
        }
    }
    
    public String addToIndex(final String indexName, final byte[] source) {
        final IndexResponse index = client.prepareIndex(indexName, "entry").setSource(source).get();
        return index.getId();
    }
    
    public boolean isClusterAvailable() {
        return clusterAvailable;
    }
}
