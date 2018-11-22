package shared.topics

object TopicTypes {
  object CommandTopic {
    val request  = "command_request"
    val response = "command_response"

    val all = List(request, response)
  }

  object ActionTopic {
    val request            = "action_request"
    val requestUnprocessed = "action_request_unprocessed"
    val response           = "action_response"

    val all = List(request, response)
  }

  object SagaTopic {
    val request          = "saga_request"
    val response         = "saga_response"
    val responseTopicMap = "saga_response_topic_map"
    val state            = "saga_state"
    val stateTransition  = "saga_state_transition"

    val all = List(request, response, state, stateTransition)
  }

  case class X[A]()
}
