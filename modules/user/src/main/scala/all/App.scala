package all

object App {

  def main(args: Array[String]): Unit = {
    command.App.startCommandProcessor()
    action.App.startSourcingActionProcessor()
    action.App.startAsyncActionProcessor()
    saga.App.startSagaCoordinator()
  }

}
