import sqlite3, re, os, socket
from time import gmtime, strftime
import irc.bot
import irc.buffer
import pdb

MAX_RESULTS = 20
LOG_FILE = 'lserve.log'
BOOKS_PATH = 'books\\'
DB_PATH = 'books.db'
NICK = 'LannyBot'
REAL = 'lan.rogers.book@gmail.com'
SERVER = 'LosAngeles.CA.US.Undernet.Org:6667'
CHANNEL = '#bookz'
LOG_LEVEL = 1 # 0 - log nothing, 1 - log requests and privmessages, 2 - log all events (lots of logs)
LOG_TO_STDOUT = True # If set, sends logs to stdout, otherwise LOG_FILE
irc.buffer.DecodingLineBuffer.errors = 'replace' # So the bot doesn't shit bricks on latin input


class queue() :
    '''DCC request queue, proabaly a built in for this kind of thing...'''
    def __init__(self) :
        self.queue = []

    def __len__(self) :
        return len(self.queue)

    def get_next(self) :
        return self.queue[0]

    def pop(self) :
        val = self.queue[0]
        self.queue = self.queue[1:]

        return val

    def push(self, item) :
        self.queue.append(item)

class LannyServ(irc.bot.SingleServerIRCBot) :
    def __init__(self, nick, server, dbConn, real=None, channel=None, blacklist=[]) :
        self.dbConn = dbConn
        self.dbCur = dbConn.cursor()
        self.queue = queue()

        host, port = server.split(':')
        self.channel = channel
        self.blacklist = blacklist

        # Map clients to requests
        self.clientMap = {}

        log('Connecting to %s on port %s' % (host, port))

        irc.bot.SingleServerIRCBot.__init__(self,
            server_list = [(host, int(port))],
            nickname = nick,
            realname = real if real else nick
        )

    def on_welcome(self, c, e)  :
        if self.channel :
            log('Connection successful, joining room...')
            c.join(self.channel)

        else :
            log('Connection successful')

    def on_join(self, c, e) :
        log('Joined channel %s successfully, listening...' % self.channel)

    def on_dcc_connect(self, connection, event):
        if self.filesize == 0:
            self.dcc.disconnect()
            return

        self.send_chunk()

    def send_chunk(self) :
        data = self.file.read(1024)
        self.dcc._custom_privmsg(self.dcc, data)
        self.sent_bytes = self.sent_bytes + len(data)


    def on_pubmsg(self, c, e) :
        message = e.arguments[0]
        sender = e.source.split('!', 1)[0]

        log('%s:%s' % (sender, message), 2)

        self.commandParser(c, e, message, sender)

    def on_privmsg(self, c, e) :
        log('%s >> %s' % (e.source, e.arguments[0]))

    def on_dccmsg(self, c, e) :
        if self.sent_bytes < self.filesize :
            self.send_chunk()

    def on_dcc_disconnect(self, c, e) :
        '''Serve next file in queue, if one exists'''
        justFinishedRequest = self.queue.pop()

        # File transfer may or may not have been successful
        if self.sent_bytes == self.filesize :
            # Update records
            self.dbCur.execute('UPDATE books SET served=? WHERE title=?', [justFinishedRequest['served']+1, justFinishedRequest['served']])
            self.dbConn.commit()
            log('File %s successfully transfered to %s' % (justFinishedRequest['title'], justFinishedRequest['sender']))

        if len(self.queue) :
            for i,request in enumerate(self.queue.queue) :
                # Alert the everyone still in the queue
                self.connection.privmsg(request['sender'], 'You\'ve moved to #%d in my queue' % i)

            self.dcc_send(self.queue.get_next())

    def dcc_send(self, queueItem) :
        self.receiver = queueItem['sender']
        self.filename = queueItem['path']
        self.filesize = os.path.getsize(self.filename)
        self.file = open(self.filename, 'rb')
        self.sent_bytes = 0

        # Initiate a DCC send
        self.dcc = self.dcc_listen("raw")
        self.dcc._custom_privmsg = _custom_privmsg
        self.connection.ctcp("DCC", self.receiver, "SEND %s %s %d %d" % (
            os.path.basename(self.filename.replace(' ', '_')),
            irc.client.ip_quad_to_numstr(self.dcc.localaddress),
            self.dcc.localport,
            self.filesize))

    def commandParser(self, c, e, command, sender) :
        if sender in self.blacklist :
            return

        elif re.match('@find .+', command) :
            # Search database for matching books and PM them to the sender
            keywords = ['%%%s%%'.lower() % x for x in command[6:].split()]
            query = 'SELECT * FROM books WHERE '

            queryChunks = ['keywords LIKE ?' for x in keywords]
            query += ' AND '.join(queryChunks)
            # Directly making the string here is OK because we know MAX_REULTS is safe
            query += 'LIMIT %d' % MAX_RESULTS

            self.dbCur.execute(query, keywords)
            results = self.dbCur.fetchall()

            if len(results) > MAX_RESULTS :
                c.privmsg(sender, 'Your search found too many matches, here are the first %d, try narrowing your search' % MAX_RESULTS)
                results = results[:MAX_RESULTS]

            elif len(results) < 1 :
                log('Search for %s returned no matches' % command[6:], 2)
                return

            c.privmsg(sender, 'I have found %d matches for your search for "%s"' % (len(results), ' '.join(keywords)))

            for result in results :
                c.privmsg(sender, '!%s %s' % (c.nickname, result['title']))

                # And update the search count
                self.dbCur.execute('UPDATE books SET searches_matched=? WHERE title=?', [result['searches_matched']+1, result['title']])

            self.dbConn.commit()
            log('Returned %d results for query %s' % (len(results), command[6:]), 2)

        elif re.match('!%s .+' % c.nickname, command) :
            title = command[len(c.nickname)+2:]
            self.dbCur.execute('SELECT * FROM books WHERE title=?', [title])
            result = self.dbCur.fetchone()

            if not result :
                c.privmsg(sender, 'Sorry, the book you requested doesn\'t seem to exist')
                log('Nonexistant book requested: %s' % title)
                return

            self.dbCur.execute('UPDATE books SET requested=? WHERE title=?', [result['requested']+1, title])
            self.dbConn.commit()

            result['sender'] = sender

            self.queue.push(result)
            c.privmsg(sender, 'Your request has been added as #%d in my queue' % len(self.queue))
            log('%s has requested %s and is #%d in the queue' % (sender, result['title'], len(self.queue)))

            if len(self.queue) == 1 :
                self.dcc_send(self.queue.get_next())

    def _dispatcher(self, connection, event):
        # Custom dispatcher, mostly for debugging
        log(event.type, 2)

        m = "on_" + event.type
        if hasattr(self, m):
            getattr(self, m)(connection, event)


def _custom_privmsg(self, data) :
    """Custom privmsg for sending DCC data"""
    try:
        self.socket.send(data)
        if self.dcctype == "chat":
            self.socket.send("\n")
            log('Sent %d bytes of data over DCC' % len(data), 2)

    except socket.error:
        # Ouch!
        self.disconnect("Connection reset by peer.")

def updateDb(path, conn) :
    # Iterate through files in path
    c = conn.cursor()

    for book in os.listdir(path) :
        c.execute('SELECT * FROM books WHERE path=?', [os.path.join(path, book)])

        if not c.fetchall() :
            bookPath = os.path.join(path, book)
            # Haha, bet you wish you knew what was going on here!
            # kewords is a coma seperated  lower case list of all words in book title longer than one character. Whew!
            keywords = ','.join([x.lower() for x in filter(lambda x: len(x) > 1, book.split())])
            c.execute('INSERT INTO books (title, keywords, searches_matched, \
                requested, served, path) VALUES (?,?,?,?,?,?)', 
                [book, keywords, 0, 0, 0, bookPath])

            log('Added new book: %s' % book)

    conn.commit()

def createDb(path) :
    '''Create new sqlite database at path with no entries, returns connection object'''
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute('CREATE TABLE books (title TEXT, keywords TEXT, searches_matched NUMERIC, requested NUMERIC, served NUMERIC, path TEXT)')
    c.execute('CREATE TABLE download_records (requester TEXT, initiated TEXT, completed TEXT, book NUMERIC)')

    conn.commit()

    return conn

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def log(line, minimum_log_level=1) :
    if minimum_log_level > LOG_LEVEL :
        return

    line = '%s: %s' % (strftime("%a, %d %b %Y %H:%M:%S GMT", gmtime()), line)
    if LOG_TO_STDOUT :
        print line

    else : 
        log_file.write(line+'\n')


if __name__ == '__main__' :
    if not LOG_TO_STDOUT:
        log_file = open(LOG_FILE, 'a', 1)

    if os.path.exists(DB_PATH) :
        conn = sqlite3.connect(DB_PATH)
    else :
        conn = createDb(DB_PATH)

    # Make our DB throw dictionary-like objects back at us
    conn.row_factory = dict_factory
    updateDb(BOOKS_PATH, conn)

    bot = LannyServ(nick=NICK, 
        real=REAL, 
        server=SERVER,
        channel=CHANNEL,
        dbConn=conn
    )

    bot.start()
