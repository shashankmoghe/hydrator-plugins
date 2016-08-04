package co.cask.hydrator.plugin.common;

import org.apache.sshd.server.Command;
import org.apache.sshd.server.CommandFactory;

public class EchoCommandFactory implements CommandFactory {

  public static final EchoCommandFactory INSTANCE = new EchoCommandFactory();

  private EchoCommandFactory(){}

  @Override
  public Command createCommand(String command) {
    return new EchoCommand(command);
  }
}
