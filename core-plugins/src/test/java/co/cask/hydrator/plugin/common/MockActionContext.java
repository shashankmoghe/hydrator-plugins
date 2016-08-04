package co.cask.hydrator.plugin.common;


import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.security.store.SecureStoreData;
import co.cask.cdap.api.security.store.SecureStoreMetadata;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.cdap.etl.api.action.SettableArguments;
import co.cask.cdap.etl.batch.customaction.BasicSettableArguments;
import co.cask.cdap.proto.id.NamespaceId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockActionContext implements ActionContext {

  private SettableArguments settableArguments;

  public MockActionContext() {
    this.settableArguments = new BasicSettableArguments(new HashMap<String, String>());
  }

  @Override
  public long getLogicalStartTime() {
    return 0L;
  }

  @Override
  public SettableArguments getArguments() {
    return settableArguments;
  }

  @Override
  public PluginProperties getPluginProperties(String pluginId) {
    return null;
  }

  @Override
  public <T> Class<T> loadPluginClass(String pluginId) {
    return null;
  }

  @Override
  public <T> T newPluginInstance(String pluginId) throws InstantiationException {
    return null;
  }

  @Override
  public <T> T newPluginInstance(String pluginId, MacroEvaluator evaluator) {
    return null;
  }

  @Override
  public String getNamespace() {
    return NamespaceId.DEFAULT.getNamespace();
  }

  @Override
  public List<SecureStoreMetadata> listSecureData(String namespace) {
    return null;
  }

  @Override
  public SecureStoreData getSecureData(String namespace, String name) {
    return null;
  }

  @Override
  public void putSecureData(String namespace, String name, String data, String description, Map<String, String> properties) {
    // no-op; unused
  }

  @Override
  public void deleteSecureData(String namespace, String name) {
    // no-op; unused
  }

  @Override
  public void execute(TxRunnable runnable) {
    // no-op; unused
  }
}
