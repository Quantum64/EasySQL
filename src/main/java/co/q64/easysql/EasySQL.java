 package co.q64.easysql;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;

public class EasySQL extends JavaPlugin {

	private static DatabaseManager db;

	public void onEnable() {
		Bukkit.getLogger().info("[EasySQL] EasySQL has been enabled");
	}

	public void onDisable() {
		Bukkit.getLogger().info("[EasySQL] EasySQL has been disabled");
	}

	public static DatabaseManager getInstance() {
		if (db == null) {
			db = new DatabaseManager();
		}
		return db;
	}
}
