"""
dojo_teachings.py — the Too Nice for Sales teaching bank for Dojo emails.

Extracted from Barry's manuscript (Too Nice for Sales, 2021) so every
Monday training email teaches HIS frameworks in HIS words. Quotes are
verbatim from the book; summaries are faithful compressions. Keyed by
Dojo focus. Used as grounding for the LLM teach paragraph, and the
'quote' can be dropped in directly.
"""

TEACHINGS = {
    "the_ask": {
        "chapters": "Ch. 3 (Creating an Opportunity) + Ch. 4 (Creating a Genuine Need)",
        "principles": [
            "Your prospects don't know what they don't know. They do actually "
            "need you; they just don't know it. Don't trust their opinion of "
            "your worth, because you're the expert, not them.",
            "Be a resource: you show value by actually teaching the person "
            "things they don't know about home buying. Provide so much "
            "direction, information, and insight that a person would be crazy "
            "to choose anyone else.",
            "People don't have a plan, even when they say they do. When "
            "someone says they will buy in twelve months, there is a 92 "
            "percent chance they won't stick to that plan. The prospect needs "
            "you today, not in twelve months.",
            "You aren't selling a house when you talk to someone early in "
            "their journey; you are selling your value. Done right, the "
            "long-term nurture goes from nine to twelve months down to three "
            "to four.",
        ],
        "quote": ("Add value first. Creating a need for you is the key to "
                  "speaking with someone early in their buying or selling "
                  "process."),
        "script_beat": ("Teach one thing they don't know, then ask. The ask "
                        "lands because the value came first."),
    },
    "objections": {
        "chapters": "Ch. 7 (The Socratic Method of Selling)",
        "principles": [
            "Handle objections with a compliment plus a curious question. "
            "'I can't buy until I save enough money' becomes: 'I understand. "
            "It's great you are setting money aside. Just curious, where did "
            "you find out how much money you needed to save?'",
            "Be curious in an endearing way. Curiosity is a choice. Decide to "
            "be interested, and you'll automatically have more natural "
            "conversations.",
            "It's not about you or your paycheck. When you're timid or shy, "
            "you're focused on your own concern instead of the person "
            "preparing to make the largest financial transaction of their "
            "life. The less you-focused you become, the more your conversion "
            "numbers skyrocket.",
            "People remember 90 percent of how you made them feel and 10 "
            "percent of what you said.",
        ],
        "quote": ("Push your insecurities to the side so you can focus on "
                  "being an active listener and an aggressive helper."),
        "script_beat": ("When the bot pushes back: compliment the concern, "
                        "then one curious question, then silence."),
    },
    "fundamentals": {
        "chapters": "Ch. 1 (How to View an Opportunity) + Ch. 7 mindset principles",
        "principles": [
            "How you view the opportunity decides the call. When you're "
            "grateful, positive, and optimistic, you outsell your coworkers "
            "without even trying. Complaining about leads lowers your sales "
            "performance every time.",
            "The Socratic questions that convert: What are you hoping to "
            "change about where you live when you do end up moving? Do you "
            "happen to have a friend in the real estate business? What would "
            "be the perfect scenario for your purchase?",
            "Don't rush your prospects, and don't lean on sleazy urgency. A "
            "script that doesn't force buying right away earns more trust and "
            "more deals over time.",
        ],
        "quote": ("When an opportunity is given to you, give it everything "
                  "you have. You don't know how many solid pitches you'll get "
                  "in life, so when you see one, swing for the fences."),
        "script_beat": ("Run the skeleton: connect, get curious, teach one "
                        "thing, then ask for the meeting."),
    },
    "get_on_the_board": {
        "chapters": "Ch. 4, Point Four (Find Your 5 Percent)",
        "principles": [
            "Five out of one hundred contacts will transact in the next six "
            "to twelve months. Connect with five out of a hundred people this "
            "month and you have an amazing career ahead of you.",
            "That means ninety-five times someone won't answer or won't be "
            "interested. Treat the nos like water flowing through the gold "
            "pan: stay laser-focused on finding the gold.",
            "If you don't talk to more people in a week, you simply won't "
            "convert many online leads. The key is having more opportunities "
            "every month.",
        ],
        "quote": ("It's not about finding hot leads. It's not about making a "
                  "sale. It's finding those five people who need our help, and "
                  "we hunt for them like we are hunting for gold."),
        "script_beat": ("The practice line is the warm-up lap: two minutes, "
                        "zero stakes, and the first real dial stops being the "
                        "scariest part of the day."),
    },
    "sharpen": {
        "chapters": "Ch. 8 (Act Like You Care) + Ch. 4 (Be a Resource)",
        "principles": [
            "Standing out isn't 'I sell a million homes a year.' It's being "
            "aggressively helpful: the salesperson who added value during a "
            "hard weekend won the deal, because simply put, he cared.",
            "Agents who can convert top-of-funnel leads early in their "
            "journey will be the ones who make a great living over the next "
            "decade. Talking with people just beginning the process is a "
            "learned skill, and it's the moat.",
        ],
        "quote": ("Authenticity sells more than anything else in our culture "
                  "today. When you represent yourself as an authentic human, "
                  "people will feel it."),
        "script_beat": ("You've earned the harder room. Master the edge cases "
                        "and the everyday calls become automatic."),
    },
}
