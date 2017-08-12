Embulk::JavaPlugin.register_output(
  "orc", "org.embulk.output.orc.OrcOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
