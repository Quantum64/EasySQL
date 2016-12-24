package co.q64.easysql;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractPlayerData {

	public AtomicBoolean needsUpdate = new AtomicBoolean();

	public abstract String getTableName();

	public abstract String getName();

	public abstract void setName(String name);

	public abstract String getPlayer();

	public abstract void setPlayer(String player);
}
