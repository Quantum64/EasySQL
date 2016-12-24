package co.q64.easysql;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class DatabaseManager implements Listener {

	public static final String DEFAULT_NAME = "default";
	private final String DRIVER_NAME = "com.mysql.jdbc.Driver";

	private List<Class<? extends AbstractPlayerData>> tables = new ArrayList<Class<? extends AbstractPlayerData>>();
	private Map<Class<?>, List<AbstractPlayerData>> data = new ConcurrentHashMap<Class<?>, List<AbstractPlayerData>>();
	private QueryRunner runner;
	private ScheduledExecutorService updater = Executors.newSingleThreadScheduledExecutor();
	private ExecutorService updateTasks = Executors.newCachedThreadPool();

	protected DatabaseManager() {

	}

	public void addTable(Class<? extends AbstractPlayerData> clazz) {
		tables.add(clazz);
	}

	public boolean init() {
		Bukkit.getPluginManager().registerEvents(this, JavaPlugin.getPlugin(EasySQL.class));
		try {
			this.runner = new QueryRunner(getDataSource());

			boolean success = DbUtils.loadDriver(DRIVER_NAME);
			if (!success) {
				Bukkit.getLogger().severe("[EasySQL] Could not initilize the SQL Driver");
				return false;
			}
			if (!createTables()) {
				Bukkit.getLogger().severe("[EasySQL] Could create tables");
			}
		} catch (Exception e) {
			Bukkit.getLogger().severe("[EasySQL] Could not init SQL");
			e.printStackTrace();
			return false;
		}
		updater.scheduleAtFixedRate(new DataUpdateTask(), 500L, 500L, TimeUnit.MILLISECONDS);
		return true;
	}

	public <T> T getData(Class<T> dataType, UUID player) {
		List<AbstractPlayerData> list = getDataList(dataType);
		if (list == null) {
			return null;
		}
		for (AbstractPlayerData pd : list) {
			if (pd.getPlayer().equalsIgnoreCase(player.toString())) {
				if (pd.getName().isEmpty()) {
					Player p = Bukkit.getPlayer(player);
					if (p != null) {
						pd.setName(p.getName());
					}
				}
				return dataType.cast(pd);
			}
		}
		try {
			Bukkit.getScheduler().scheduleSyncDelayedTask(JavaPlugin.getPlugin(EasySQL.class), new Runnable() {

				@Override
				public void run() {
					Player p = Bukkit.getPlayer(player);
					if (p != null) {
						p.kickPlayer(ChatColor.DARK_RED + "There was a critical error handling a type: " + ChatColor.RED + dataType.getSimpleName() + "\n" + ChatColor.RED + "This probably happened because you attempted to join while the server was still starting");
					}
				}
			});
			AbstractPlayerData a = AbstractPlayerData.class.cast(dataType.getDeclaredConstructor().newInstance());
			a.setPlayer(player.toString());
			a.setName(DEFAULT_NAME);
			return dataType.cast(a);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		return null;
	}

	public <T> List<AbstractPlayerData> getDataList(Class<T> dataType) {
		if (!AbstractPlayerData.class.isAssignableFrom(dataType)) {
			Bukkit.getLogger().severe("Could not get data instance because " + dataType.getSimpleName() + " is not an instance of data!");
			return null;
		}
		List<AbstractPlayerData> list = data.get(dataType);
		if (list == null) {
			list = new CopyOnWriteArrayList<AbstractPlayerData>();
			data.put(dataType, list);
			// Console.log("Added new data list for data object type " +
			// dataType.getSimpleName());
		}
		return list;
	}

	public void disconnect(UUID u) {
		for (Class<?> clazz : data.keySet()) {
			@SuppressWarnings("unchecked")
			Class<AbstractPlayerData> casted = (Class<AbstractPlayerData>) clazz;
			if (data.get(clazz).remove(getData(casted, u))) {
				// Console.log("Removed " + clazz.getSimpleName() + " for " +
				// u.toString() + " on disconnect");
			} else {
				Bukkit.getLogger().warning("Did NOT remove " + clazz.getSimpleName() + " for " + u.toString() + " on disconnect");
			}
		}
	}

	private void queryData(UUID u) {
		try {
			ExecutorService executor = Executors.newCachedThreadPool();
			for (Class<? extends AbstractPlayerData> data : tables) {
				Future<?> task = executor.submit(new GetData(u, getTableName(data), new BeanHandler<>(data), data));
				executor.execute(new TimeoutGet(task, 4));
			}
			executor.shutdown();
			executor.awaitTermination(6, TimeUnit.SECONDS);

		} catch (InterruptedException e) {
			Bukkit.getLogger().severe("[EasySQL] Player login thread interrupted unexpectedly!");
			e.printStackTrace();
		}
	}

	private Map<String, Object> introspect(Object obj) {
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		try {
			Class<?> clazz = obj.getClass();
			if (!clazz.getSuperclass().getName().equals(AbstractPlayerData.class.getName())) {
				clazz = clazz.getSuperclass();
			}
			List<Field> declaredFields = Arrays.asList(clazz.getDeclaredFields());
			for (Field f : declaredFields) {
				f.setAccessible(true);
				Object value = f.get(obj);
				result.put(f.getName(), value);
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return result;
	}

	private boolean createTables() {
		boolean success = true;
		for (Class<? extends AbstractPlayerData> data : tables) {
			if (!createTable(getTableName(data), data)) {
				success = false;
			}
		}
		return success;
	}

	public String getTableName(Class<? extends AbstractPlayerData> data) {
		try {
			AbstractPlayerData inst = data.newInstance();
			return inst.getTableName();
		} catch (IllegalAccessException | IllegalArgumentException | SecurityException | InstantiationException e) {
			Bukkit.getLogger().severe("[EasySQL] Error reflecting table name!");
			e.printStackTrace();
		}
		return null;
	}

	private boolean createTable(String name, Class<?> columns) {
		List<Field> declaredFields = Arrays.asList(columns.getDeclaredFields());
		StringBuilder values = new StringBuilder();
		values.append("(");
		for (Field f : declaredFields) {
			values.append("`");
			values.append(f.getName());
			values.append("`");
			if (f.getType().isAssignableFrom(String.class)) {
				if (f.getName().endsWith("L")) {
					values.append(" mediumtext");
				} else {
					values.append(" varchar(255)");
				}
			} else if (f.getType().isAssignableFrom(float.class)) {
				values.append(" float");
			} else if (f.getType().isAssignableFrom(int.class)) {
				values.append(" int");
			} else if (f.getType().isAssignableFrom(long.class)) {
				values.append(" bigint");
			} else {
				continue;
			}
			if (declaredFields.indexOf(f) == (declaredFields.size() - 1)) {
				values.append(", PRIMARY KEY(player))");
			} else {
				values.append(", ");
			}
		}
		StringBuilder statement = new StringBuilder();
		statement.append("CREATE TABLE IF NOT EXISTS ");
		statement.append(name);
		statement.append(" ");
		statement.append(values.toString());
		statement.append(" ENGINE=InnoDB DEFAULT CHARSET=utf8");
		try {
			runner.update(statement.toString());
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private MysqlDataSource getDataSource() {
		try {
			File cfg = new File(JavaPlugin.getPlugin(EasySQL.class).getDataFolder(), "SQL.yml");
			if (!cfg.exists()) {
				cfg.createNewFile();
				PrintWriter pw = new PrintWriter(cfg);
				pw.println("# SQL Config");
				pw.println("sql.host: host");
				pw.println("sql.password: password");
				pw.println("sql.port: port");
				pw.println("sql.username: username");
				pw.println("sql.database: database");
				pw.flush();
				pw.close();
			}
			FileConfiguration config = YamlConfiguration.loadConfiguration(cfg);
			String path = "sql.";
			String hostname = config.getString(path + "host");
			String password = config.getString(path + "password");
			String port = config.getString(path + "port");
			String user = config.getString(path + "username");
			String database = config.getString(path + "database");
			MysqlDataSource ds = new MysqlDataSource();
			ds.setUrl("jdbc:mysql://" + hostname + ":" + port + "/" + database);
			ds.setUser(user);
			ds.setPassword(password);
			ds.setCharacterEncoding("UTF-8");
			return ds;
		} catch (NullPointerException e) {
			Bukkit.getLogger().severe("[EasySQL]Could not read SQL config");
		} catch (IOException e) {
			Bukkit.getLogger().severe("[EasySQL]Could not write SQL config");
		}
		return null;
	}

	private void addPlayerData(final AbstractPlayerData add) {
		AbstractPlayerData enhanced = AbstractPlayerData.class.cast(Enhancer.create(add.getClass(), new SetterInvocationHandler()));
		Class<?> dataClass = enhanced.getClass();
		for (Entry<String, Object> field : introspect(add).entrySet()) {
			try {
				Field f = dataClass.getSuperclass().getDeclaredField(field.getKey());
				f.setAccessible(true);
				f.set(enhanced, field.getValue());
			} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
				e.printStackTrace();
				Bukkit.getLogger().severe("[EasySQL] Error cloning data object to proxy");
			}
		}
		// for (Field f : dataClass.getDeclaredFields()) {
		// Console.log(f.getName());
		// }
		List<AbstractPlayerData> list = data.get(dataClass.getSuperclass());
		if (list == null) {
			list = new ArrayList<AbstractPlayerData>();
			data.put(dataClass.getSuperclass(), list);
			// Console.log("Added new data list for data object type " +
			// dataClass.getSimpleName());
		}
		// Console.log("Added player data type " + dataClass.getSimpleName() +
		// " for " + enhanced.getName());
		list.add(enhanced);
		Bukkit.getScheduler().scheduleSyncDelayedTask(JavaPlugin.getPlugin(EasySQL.class), new Runnable() {

			@Override
			public void run() {
				Player p = Bukkit.getPlayer(UUID.fromString(enhanced.getPlayer()));
				if (p != null) {
					enhanced.setName(p.getName());
				}
			}
		}, 10L);
	}

	@EventHandler(priority = EventPriority.LOW)
	public void onPlayerJoin(AsyncPlayerPreLoginEvent event) {
		queryData(event.getUniqueId());
	}

	@EventHandler(priority = EventPriority.NORMAL)
	public void onPlayerQuit(PlayerQuitEvent event) {
		disconnect(event.getPlayer().getUniqueId());
	}

	public class UpdateData implements Runnable {

		private AbstractPlayerData data;
		private String tableName;

		public UpdateData(AbstractPlayerData data, String tableName) {
			this.data = data;
			this.tableName = tableName;
		}

		@Override
		public void run() {
			try {
				boolean exists = runner.query("SELECT * FROM " + tableName + " WHERE player=?", new ResultSetHandler<Boolean>() {
					@Override
					public Boolean handle(ResultSet rs) throws SQLException {
						return rs.next();
					}
				}, data.getPlayer());
				Map<String, Object> introspected = introspect(data);
				if (exists) {
					if (!data.getName().equals(DEFAULT_NAME)) {
						StringBuilder statement = new StringBuilder();
						statement.append("UPDATE ");
						statement.append(tableName);
						statement.append(" SET ");
						Iterator<String> keys = introspected.keySet().iterator();
						while (keys.hasNext()) {
							String s = keys.next();
							statement.append(s);
							if (keys.hasNext()) {
								statement.append("=?, ");
							} else {
								statement.append("=? WHERE player=");
								statement.append("'");
								statement.append(data.getPlayer());
								statement.append("'");
							}
						}
						runner.update(statement.toString(), introspected.values().toArray());
					}
				} else {
					StringBuilder statement = new StringBuilder();
					statement.append("INSERT INTO ");
					statement.append(tableName);
					statement.append(" VALUES(");
					Iterator<String> keys = introspected.keySet().iterator();
					while (keys.hasNext()) {
						keys.next();
						if (keys.hasNext()) {
							statement.append("?, ");
						} else {
							statement.append("?)");
						}
					}
					runner.update(statement.toString(), introspected.values().toArray());
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public class GetData implements Runnable {

		private UUID player;
		private String tableName;
		private BeanHandler<? extends AbstractPlayerData> definedHandler;
		private Class<? extends AbstractPlayerData> bean;

		public GetData(UUID player, String tableName, BeanHandler<? extends AbstractPlayerData> definedHandler, Class<? extends AbstractPlayerData> bean) {
			this.player = player;
			this.definedHandler = definedHandler;
			this.tableName = tableName;
			this.bean = bean;
		}

		@Override
		public void run() {
			try {
				AbstractPlayerData data = runner.query("SELECT * FROM " + tableName + " WHERE player=?", definedHandler, player.toString());
				if (data == null) {
					try {
						data = bean.newInstance();
					} catch (InstantiationException | IllegalAccessException e) {
						e.printStackTrace();
					}
					data.setPlayer(player.toString());
					data.setName(DEFAULT_NAME);
				}
				addPlayerData(data);
			} catch (SQLException e) {
				Bukkit.getLogger().severe("Problem getting player data:" + e.getMessage());
				e.printStackTrace();
			}
		}
	}

	private class TimeoutGet implements Runnable {

		private Future<?> task;
		private int timeout;

		public TimeoutGet(Future<?> task, int timeout) {
			this.task = task;
			this.timeout = timeout;
		}

		@Override
		public void run() {
			try {
				task.get(timeout, TimeUnit.SECONDS);
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				Bukkit.getLogger().warning("[EasySQL] A task timed out!");
				e.printStackTrace();
			}
		}
	}

	private class SetterInvocationHandler implements MethodInterceptor {

		@Override
		public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
			Object result = proxy.invokeSuper(obj, args);
			if (method.getName().startsWith("set") && !method.getName().equals("setPlayer") && !method.getName().equals("setName")) {
				if (AbstractPlayerData.class.isAssignableFrom(obj.getClass())) {
					AbstractPlayerData data = AbstractPlayerData.class.cast(obj);
					data.needsUpdate.set(true);
				} else {
					Bukkit.getLogger().warning("[EasySQL] An intercepted method from " + method.getDeclaringClass().getSimpleName() + " was not an instance of AbstractBeanPlayerData!");
				}
			}
			return result;
		}
	}

	private class DataUpdateTask implements Runnable {

		@Override
		public void run() {
			for (List<AbstractPlayerData> list : data.values()) {
				for (AbstractPlayerData data : list) {
					if (data.needsUpdate.compareAndSet(true, false)) {
						// Console.log("Updated " +
						// data.getClass().getSimpleName() + " data for " +
						// data.getName());
						updateTasks.submit(new UpdateData(data, data.getTableName()));
					}
				}
			}
		}
	}
}
