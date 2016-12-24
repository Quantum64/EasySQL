package co.q64.easysql;

import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

public class Example extends JavaPlugin {

	private DatabaseManager db;

	public void onEnable() {
		// First get the database manager
		db = EasySQL.getInstance();
		// We need to register our bean classes with the manager before we init
		// it
		db.addTable(Coins.class);
		// Next we try to init the db manager, if something fails, like
		// connecting to the database, this will return false
		if (!db.init()) {
			Bukkit.getLogger().severe("We can't connect to the database!  That's a shame");
			Bukkit.getPluginManager().disablePlugin(this);
			return;
		}
	}

	public void addSomeCoins(int coins, Player p) {
		db.getData(Coins.class, p.getUniqueId()).addCoins(coins);
	}

	public void removeSomeCoins(int coins, Player p) {
		if (!db.getData(Coins.class, p.getUniqueId()).removeCoins(coins)) {
			p.sendMessage("You need more coins to buy that!");
		}
	}

	// Data classes need to be to the java bean spec
	public class Coins extends AbstractPlayerData {

		// Java beans need private fields that are not initialized here
		private String player;
		private String name;
		private int coins;

		// Java beans have a 0 argument constructor
		public Coins() {
			// You initialize your variables here
			this.player = "";
			this.name = "";
			this.coins = 0;
		}

		public String getTableName() {
			// This is the name of the MySQL table
			return "Coins";
		}

		public String getPlayer() {
			// UUID of player in string form, usually not needed
			return player;
		}

		public void setPlayer(String player) {
			// This should not be manually changed
			this.player = player;
		}

		public String getName() {
			// Name of the player in string form
			return name;
		}

		public void setName(String name) {
			// This should not be manually changed
			this.name = name;
		}

		// These are the methods we added for this data object
		public int getCoins() {
			return coins;
		}

		public void setCoins(int coins) {
			this.coins = coins;
		}

		public void addCoins(int amount) {
			setCoins(getCoins() + amount);
			// It is important that you do not
			// set values directly, you need to
			// use the setter because that's
			// what ASM is looking for
		}

		public boolean removeCoins(int amount) {
			if (getCoins() >= amount) {
				setCoins(getCoins() - amount);
				return true;
			}
			return false;
		}
	}
}
