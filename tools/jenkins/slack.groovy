def notifySlack(String channel, String additionalMessage = '') {
    // Send a build status notification to Slack.
    // Differentiate status visually using both color and shape.
    // Fail the build if we can't send the notification so that
    // we won't have silent failures lurking.

    def status = currentBuild.currentResult
    def color
    def emoji

    if (status == 'SUCCESS') {
        color = 'good'
        emoji = ':white_check_mark:'
    } else if (status == 'UNSTABLE') {
        color = 'warning'
        emoji = ':warning:'
    } else {
        color = 'danger'
        emoji = ':x:'
    }

    // The build isn't quite done yet so strip the " and counting" off.
    def message = "$emoji - ${currentBuild.fullDisplayName} - <${env.BUILD_URL}|${status}> " +
        "after ${currentBuild.durationString.minus(' and counting')}"
    if (additionalMessage != "") {
        message += '\n' + additionalMessage
    }

    slackSend channel: channel,
              color: color,
              message: message,
              failOnError: true
}

return this
