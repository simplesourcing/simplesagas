package all
import org.rocksdb.RocksDB

object App {

  def main(args: Array[String]): Unit = {

    RocksDB.loadLibrary()

    command.App.startCommandProcessor()
    action.App.startSourcingActionProcessor()
    action.App.startAsyncActionProcessor()
    saga.App.startSagaCoordinator()
  }

}
