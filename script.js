document.addEventListener('DOMContentLoaded', () => {
    // ===== Apps Script URL (for WRITE operations only) =====
    const SCRIPT_URL = 'https://script.google.com/macros/s/AKfycbx2A8eK6bbH73380G0qW2WJH9RKBAxvqlGIAJf8k35iwBKtW3X0cZo4FRW4ag4OmzVG/exec';

    // ===== TSV Data Sources (for FAST READ operations) =====
    const TSV_BASE_URL = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTl1nnZ64lU36_IvLrFdN0JVLkn9X1hpP_g_IQK7i34YmsuMg_DFYw6Uhf9Ru420VE8nwp0JLAsWr6Y/pub';
    const TSV_SHEETS = {
        accounts: { gid: 0, url: `${TSV_BASE_URL}?gid=0&single=true&output=tsv` },
        posts: { gid: 1260057010, url: `${TSV_BASE_URL}?gid=1260057010&single=true&output=tsv` },
        comments: { gid: 1288301970, url: `${TSV_BASE_URL}?gid=1288301970&single=true&output=tsv` },
        likes: { gid: 2005421782, url: `${TSV_BASE_URL}?gid=2005421782&single=true&output=tsv` },
        followers: { gid: 196890202, url: `${TSV_BASE_URL}?gid=196890202&single=true&output=tsv` },
        messages: { gid: 1861161898, url: `${TSV_BASE_URL}?gid=1861161898&single=true&output=tsv` },
        notifications: { gid: 1652933657, url: `${TSV_BASE_URL}?gid=1652933657&single=true&output=tsv` },
        blocks: { gid: 1228482897, url: `${TSV_BASE_URL}?gid=1228482897&single=true&output=tsv` },
        bans: { gid: 1624591656, url: `${TSV_BASE_URL}?gid=1624591656&single=true&output=tsv` },
        servInfo: { gid: 138253995, url: `${TSV_BASE_URL}?gid=138253995&single=true&output=tsv` },
        filter: { gid: 316069085, url: `${TSV_BASE_URL}?gid=316069085&single=true&output=tsv` },
        reports: { gid: 1234625074, url: `${TSV_BASE_URL}?gid=1234625074&single=true&output=tsv` },
        groupLastRead: { gid: 1004747007, url: `${TSV_BASE_URL}?gid=1004747007&single=true&output=tsv` },
        photoLibrary: { gid: 1988484974, url: `${TSV_BASE_URL}?gid=1988484974&single=true&output=tsv` }
    };

    // ===== TSV Parser Utility =====
    const tsvParser = {
        parse(tsvText) {
            if (!tsvText || tsvText.trim() === '') return [];
            const lines = tsvText.split('\n').filter(line => line.trim() !== '');
            if (lines.length === 0) return [];

            const headers = lines[0].split('\t').map(h => h.trim());
            const data = [];

            for (let i = 1; i < lines.length; i++) {
                const values = lines[i].split('\t');
                const row = {};
                headers.forEach((header, index) => {
                    row[header] = values[index] || '';
                });
                data.push(row);
            }
            return data;
        },

        async fetchSheet(sheetKey) {
            const sheet = TSV_SHEETS[sheetKey];
            if (!sheet) throw new Error(`Unknown sheet: ${sheetKey}`);

            const response = await fetch(sheet.url + '&cachebust=' + Date.now());
            if (!response.ok) throw new Error(`Failed to fetch ${sheetKey}: ${response.status}`);
            const text = await response.text();
            return this.parse(text);
        },

        async fetchAllSheets() {
            const sheetKeys = Object.keys(TSV_SHEETS);
            const results = await Promise.all(sheetKeys.map(key => this.fetchSheet(key)));

            const data = {};
            sheetKeys.forEach((key, index) => {
                data[key] = results[index];
            });
            return data;
        }
    };

    const getColumn = (row, ...keys) => {
        for (const key of keys) {
            if (row[key] !== undefined && row[key] !== '') return row[key];
            const foundKey = Object.keys(row).find(k => k.trim().toLowerCase() === key.toLowerCase());
            if (foundKey) return row[foundKey];
        }
        return undefined;
    };

    // ===== Data Aggregation =====
    const dataAggregator = {
        async getConversationHistory({ userId, otherUserId }) {
            try {
                const [messagesData, accounts] = await Promise.all([
                    tsvParser.fetchSheet('messages'),
                    tsvParser.fetchSheet('accounts')
                ]);
                const messages = [];
                const currentUserId = userId;

                const userMap = {};
                accounts.forEach(row => {
                    userMap[row['userID']] = row;
                });

                messagesData.forEach(row => {
                    const msgId = row['messageID'] || row['messageId'];
                    const senderId = row['senderID'] || row['senderId'];
                    const recipientId = row['recipientID'] || row['recipientId'];
                    const encodedContent = row['messageContent'];
                    const timestamp = row['timestamp'];
                    const isRead = row['isRead'];

                    let isMatch = false;
                    if ((senderId === currentUserId && recipientId === otherUserId) || (senderId === otherUserId && recipientId === currentUserId)) {
                        isMatch = true;
                    }

                    if (isMatch) {
                        let decodedMessage = '';
                        try {
                            decodedMessage = atob(encodedContent);
                        } catch (e) {
                            decodedMessage = 'Could not decode message.';
                        }

                        messages.push({
                            messageId: msgId,
                            senderId: senderId,
                            senderName: userMap[senderId]?.displayName || 'Unknown',
                            messageContent: decodedMessage,
                            timestamp: timestamp,
                            isRead: isRead,
                            status: 'sent'
                        });
                    }
                });

                messages.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

                return { messages };
            } catch (error) {
                console.error('Error in getConversationHistory TSV:', error);
                throw error;
            }
        },
        async getPosts(params) {
            try {
                const currentUserId = params.userId || params;
                
                const [
                    servInfo, bans, blocks, accounts, postsData,
                    comments, likes, followers, messages, groupLastRead, photoLibrary
                ] = await Promise.all([
                    tsvParser.fetchSheet('servInfo'),
                    tsvParser.fetchSheet('bans'),
                    tsvParser.fetchSheet('blocks'),
                    tsvParser.fetchSheet('accounts'),
                    tsvParser.fetchSheet('posts'),
                    tsvParser.fetchSheet('comments'),
                    tsvParser.fetchSheet('likes'),
                    tsvParser.fetchSheet('followers'),
                    tsvParser.fetchSheet('messages'),
                    tsvParser.fetchSheet('groupLastRead'),
                    tsvParser.fetchSheet('photoLibrary')
                ]);

                // Server Status Check
                let isOutage = false;
                let bannerText = '';
                let isCurrentUserOutageExempt = false;

                if (servInfo && servInfo.length > 0) {
                    const servInfoRow = servInfo[0];
                    bannerText = servInfoRow['bannerText'] || '';
                    const serverStatus = servInfoRow['serverStatus'] || '';
                    if (String(serverStatus).toLowerCase() === 'outage') {
                        isOutage = true;
                    }
                }

                const currentUserRow = accounts.find(row => row['userID'] === currentUserId);
                if (currentUserRow) {
                    const isAdmin = String(currentUserRow['isAdmin'] || 'FALSE').toUpperCase() === 'TRUE';
                    isCurrentUserOutageExempt = isAdmin;
                }

                if (isOutage && !isCurrentUserOutageExempt) {
                    return {
                        posts: [],
                        conversations: [],
                        currentUserFollowingList: [],
                        currentUserFollowersList: [],
                        blockedUsersList: [],
                        currentUserData: { isSuspended: 'OUTAGE' },
                        bannerText
                    };
                }

                // Build Maps
                const now = new Date();
                const banMap = {};
                bans.forEach(row => {
                    const username = row['username'];
                    const reason = row['reason'];
                    const endDateStr = row['endDate'];
                    if (username) {
                        if (!endDateStr) {
                            banMap[username] = { reason, endDate: 'permanent' };
                        } else {
                            const endDate = new Date(endDateStr);
                            if (!isNaN(endDate.getTime()) && endDate > now) {
                                banMap[username] = { reason, endDate: endDate.toISOString() };
                            }
                        }
                    }
                });

                const blockMap = {};
                blocks.forEach(row => {
                    const blockerId = row['blockerID'] || row['Blocker ID'];
                    const blockedId = row['blockedID'] || row['Blocked ID'];
                    if (!blockMap[blockerId]) blockMap[blockerId] = new Set();
                    blockMap[blockerId].add(blockedId);
                });

                const likesByPostMap = {};
                likes.forEach(row => {
                    const postId = row['postID'];
                    const likeId = row['likeID'];
                    const userId = row['userID'];
                    if (!likesByPostMap[postId]) likesByPostMap[postId] = [];
                    likesByPostMap[postId].push({ likeId, userId });
                });

                const commentsByPostMap = {};
                comments.forEach(row => {
                    const commentId = row['commentID'];
                    if (state.deletedCommentIds.has(commentId)) return;

                    const postId = row['postID'];
                    const userId = row['userID'];
                    const commentText = row['commentText'];
                    
                    const timestamp = getColumn(row, 'timestamp', 'Timestamp', 'time stamp') || new Date().toISOString();

                    if (!commentsByPostMap[postId]) commentsByPostMap[postId] = [];
                    commentsByPostMap[postId].push({ commentId, postId, userId, commentText, timestamp });
                });

                const followingMap = {};
                const followersMap = {};
                followers.forEach(row => {
                    const followerId = row['followerID'] || row['followerId'] || row['Follower ID'];
                    const followingId = row['followingID'] || row['followingId'] || row['Following ID'];
                    if (!followingMap[followerId]) followingMap[followerId] = [];
                    followingMap[followerId].push(String(followingId)); 
                    if (!followersMap[followingId]) followersMap[followingId] = [];
                    followersMap[followingId].push(String(followerId));
                });

                const postsByUserMap = {};
                postsData.forEach(row => {
                    const postId = row['postID'];
                    if (state.deletedPostIds.has(postId)) return; 
                    const userId = row['userID'];
                    if (!postsByUserMap[userId]) postsByUserMap[userId] = [];
                    postsByUserMap[userId].push(postId);
                });

                const userMap = {};
                accounts.forEach(row => {
                    const userId = row['userID'] || row['userId'];
                    const username = row['username'];
                    const displayName = row['displayName'];
                    const profilePictureUrl = row['profilePictureUrl'] || '';
                    const description = row['description'] || '';
                    const isVerified = row['isVerified'] || 'FALSE';
                    const postVisibility = row['firePostVisibility'] || 'Everyone';
                    
                    let rawPrivacy = getColumn(row, 'profileType', 'Profile Type', 'privacy', 'profiletype') || 'public';
                    const profilePrivacy = String(rawPrivacy).trim().toLowerCase() === 'private' ? 'private' : 'public';

                    const isAdmin = String(row['isAdmin'] || 'FALSE').toUpperCase() === 'TRUE';

                    const userPostIds = postsByUserMap[userId] || [];
                    let totalLikes = 0;
                    userPostIds.forEach(postId => {
                        totalLikes += (likesByPostMap[postId] || []).length;
                    });

                    const banDetails = banMap[username] || null;

                    userMap[userId] = {
                        userId, username, displayName, profilePictureUrl, description,
                        isVerified, banDetails, postVisibility, profilePrivacy,
                        followers: (followersMap[userId] || []).length,
                        following: (followingMap[userId] || []).length,
                        totalLikes, isAdmin
                    };
                });

                const currentUserBlockedSet = blockMap[currentUserId] || new Set();
                const currentUserFollowingList = followingMap[String(currentUserId)] || [];
                const currentUserFollowersList = followersMap[String(currentUserId)] || [];
                const blockedUsersList = Array.from(currentUserBlockedSet).map(blockedId => {
                    const user = userMap[blockedId];
                    return user ? { userId: user.userId, displayName: user.displayName, profilePictureUrl: user.profilePictureUrl } : null;
                }).filter(Boolean);

                const postMap = {};
                postsData.forEach(row => {
                    const postId = row['postID'];
                    if (state.deletedPostIds.has(postId)) return;
                    const authorId = row['userID'];
                    const postContent = row['postContent'];
                    const timestamp = row['timestamp'];
                    const isStory = String(row['story'] || 'FALSE').toUpperCase() === 'TRUE';
                    const expiryTimestamp = row['expiryTimestamp'];
                    const storyDuration = row['storyDuration'];
                    postMap[postId] = { postId, authorId, postContent, timestamp, isStory, expiryTimestamp, storyDuration };
                });

                if (state.localPendingPosts && state.localPendingPosts.length > 0) {
                    const now = Date.now();
                    state.localPendingPosts = state.localPendingPosts.filter(p => (now - new Date(p.timestamp).getTime()) < 120000);
                    state.localPendingPosts.forEach(localPost => {
                        if (!postMap[localPost.postId]) postMap[localPost.postId] = localPost;
                    });
                }

                const feedItems = {};
                Object.values(postMap).forEach(post => {
                    const authorId = post.authorId;
                    if (currentUserBlockedSet.has(authorId) || (blockMap[authorId] && blockMap[authorId].has(currentUserId))) return;
                    const author = userMap[authorId];
                    if (!author) return;

                    const isOwnPost = authorId === currentUserId;
                    const viewerFollowsAuthor = currentUserFollowingList.includes(String(authorId));
                    const authorFollowsViewer = (followingMap[authorId] || []).includes(String(currentUserId));
                    const areFriends = viewerFollowsAuthor && authorFollowsViewer;

                    let canView = false;
                    if (isOwnPost) canView = true;
                    else if (author.profilePrivacy === 'private') canView = areFriends;
                    else {
                        switch (author.postVisibility) {
                            case 'Everyone': canView = true; break;
                            case 'Followers': canView = viewerFollowsAuthor; break;
                            case 'Friends': canView = areFriends; break;
                            default: canView = true;
                        }
                    }
                    if (canView) feedItems[post.postId] = { ...post, sortTimestamp: post.timestamp };
                });

                const posts = Object.values(feedItems).map(item => {
                    const author = userMap[item.authorId];
                    if (!author || author.banDetails) return null;
                    const postCommentsRaw = commentsByPostMap[item.postId] || [];
                    const postComments = postCommentsRaw
                        .filter(c => !currentUserBlockedSet.has(c.userId) && !(blockMap[c.userId] && blockMap[c.userId].has(currentUserId)))
                        .map(c => ({ ...c, ...(userMap[c.userId] || {}) }));
                    const postLikes = likesByPostMap[item.postId] || [];
                    return { ...author, ...item, comments: postComments, likes: postLikes, reposts: [] };
                }).filter(Boolean);

                const conversationsMap = {};
                messages.forEach(row => {
                    const senderId = row['senderID'] || row['senderId'];
                    const recipientId = row['recipientID'] || row['recipientId'];
                    const messageId = row['messageID'] || row['messageId'];
                    const encodedContent = row['messageContent'];
                    const timestamp = row['timestamp'];
                    const isRead = row['isRead'];

                    let decodedMessage = '';
                    try { decodedMessage = atob(encodedContent); } catch (e) { decodedMessage = 'Could not decode message.'; }

                    let conversationId = null;
                    let otherUser = null;

                    if (senderId === currentUserId || recipientId === currentUserId) {
                        const otherUserId = senderId === currentUserId ? recipientId : senderId;
                        if (!currentUserBlockedSet.has(otherUserId) && !(blockMap[otherUserId] && blockMap[otherUserId].has(currentUserId))) {
                            conversationId = otherUserId;
                            otherUser = { ...userMap[otherUserId], isGroup: false };
                        }
                    }

                    if (conversationId && otherUser) {
                        if (!conversationsMap[conversationId]) {
                            conversationsMap[conversationId] = {
                                otherUser: otherUser, lastMessage: '', timestamp: '', unreadCount: 0, messages: []
                            };
                        }
                        const convo = conversationsMap[conversationId];
                        convo.messages.push({
                            messageId, senderId, senderName: userMap[senderId]?.displayName || 'Unknown',
                            messageContent: decodedMessage, timestamp, isRead, status: 'sent'
                        });
                        if (new Date(timestamp) > new Date(convo.timestamp || 0)) {
                            convo.lastMessage = senderId === currentUserId ? `You: ${decodedMessage}` : decodedMessage;
                            convo.timestamp = timestamp;
                        }
                    }
                });

                messages.forEach(row => {
                    const senderId = row['senderID'] || row['senderId'];
                    const recipientId = row['recipientID'] || row['recipientId'];
                    const isRead = row['isRead'];
                    if (senderId === currentUserId) return;
                    if (recipientId === currentUserId && (isRead === 'FALSE' || isRead === false)) {
                        if (conversationsMap[senderId]) {
                            conversationsMap[senderId].unreadCount = (conversationsMap[senderId].unreadCount || 0) + 1;
                        }
                    }
                });

                Object.values(conversationsMap).forEach(c => {
                    c.messages.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
                });
                const conversations = Object.values(conversationsMap).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

                const libraryImages = [];
                if (photoLibrary) {
                    photoLibrary.forEach(row => {
                        const url = row['url'] || row['URL'] || Object.values(row)[0];
                        if (url && url.startsWith('http')) libraryImages.push(url);
                    });
                }

                return {
                    posts: posts.sort((a, b) => new Date(b.sortTimestamp) - new Date(a.sortTimestamp)),
                    conversations,
                    currentUserFollowingList,
                    currentUserFollowersList,
                    blockedUsersList,
                    currentUserData: userMap[currentUserId] || null,
                    bannerText,
                    photoLibrary: libraryImages
                };
            } catch (error) {
                console.error('Error in getPosts:', error);
                throw error;
            }
        },

        async getNotifications(params) {
            try {
                const currentUserId = params.userId || params;
                const notifSheet = TSV_SHEETS.notifications;
                const notifResponse = await fetch(notifSheet.url + '&cachebust=' + Date.now());
                const notifText = await notifResponse.text();
                const [blocks, accounts, postsData] = await Promise.all([
                    tsvParser.fetchSheet('blocks'), tsvParser.fetchSheet('accounts'), tsvParser.fetchSheet('posts')
                ]);
                const blockMap = {};
                const lines = notifText.split('\n').filter(line => line.trim() !== '');
                if (lines.length > 0) lines.shift();

                const notificationsData = lines.map(line => {
                    const cols = line.split('\t');
                    return {
                        notificationId: cols[0] ? cols[0].trim() : '',
                        recipientUserId: cols[1] ? cols[1].trim() : '',
                        actorUserId: cols[2] ? cols[2].trim() : '',
                        actionType: cols[3] ? cols[3].trim() : '',
                        postId: cols[4] ? cols[4].trim() : '',
                        timestamp: cols[5] ? cols[5].trim() : '',
                        isRead: cols[6] ? cols[6].trim() : ''
                    };
                });

                blocks.forEach(row => {
                    const blockerId = row['Blocker ID'] || row['blockerId'];
                    const blockedId = row['Blocked ID'] || row['blockedId'];
                    if (!blockMap[blockerId]) blockMap[blockerId] = new Set();
                    blockMap[blockerId].add(blockedId);
                });

                const currentUserBlockedSet = blockMap[currentUserId] || new Set();
                const userMap = {};
                accounts.forEach(row => {
                    userMap[row['userID']] = { displayName: row['displayName'], profilePictureUrl: row['profilePictureUrl'] || '' };
                });
                const postAuthorMap = {};
                postsData.forEach(row => { postAuthorMap[row['postID']] = row['userID']; });

                const notifications = notificationsData
                    .filter(n => String(n.recipientUserId) === String(currentUserId))
                    .filter(n => !state.deletedNotificationIds.has(n.notificationId))
                    .filter(n => !currentUserBlockedSet.has(n.actorUserId) && !(blockMap[n.actorUserId] && blockMap[n.actorUserId].has(currentUserId)))
                    .map(n => {
                        const actorInfo = userMap[n.actorUserId] || { displayName: 'An unknown user', profilePictureUrl: '' };
                        const postAuthorId = n.postId ? postAuthorMap[n.postId] || null : null;
                        return {
                            notificationId: n.notificationId,
                            actorUserId: n.actorUserId,
                            actorDisplayName: actorInfo.displayName,
                            actorProfilePictureUrl: actorInfo.profilePictureUrl,
                            actionType: n.actionType,
                            postId: n.postId,
                            postAuthorId,
                            timestamp: n.timestamp,
                            isRead: n.isRead
                        };
                    }).reverse();

                return { status: 'success', notifications };
            } catch (error) {
                console.error('Error in getNotifications:', error);
                throw error;
            }
        },

        async getUserProfile({ userId, currentUserId }) {
            try {
                const [followers, likes, postsData, accounts, bans] = await Promise.all([
                    tsvParser.fetchSheet('followers'), tsvParser.fetchSheet('likes'), tsvParser.fetchSheet('posts'), tsvParser.fetchSheet('accounts'), tsvParser.fetchSheet('bans')
                ]);

                const followingMap = {};
                const followersMap = {};
                followers.forEach(row => {
                    const followerId = row['followerId'] || row['followerID'] || row['follower ID'];
                    const followingId = row['followingId'] || row['followingID'] || row['following ID'];
                    if (followerId && followingId) {
                        if (!followingMap[followerId]) followingMap[followerId] = [];
                        followingMap[followerId].push(followingId);
                        if (!followersMap[followingId]) followersMap[followingId] = [];
                        followersMap[followingId].push(followerId);
                    }
                });

                const likesByPostMap = {};
                likes.forEach(row => {
                    const postId = row['postID'];
                    if (!likesByPostMap[postId]) likesByPostMap[postId] = [];
                    likesByPostMap[postId].push({ likeId: row['likeID'], userId: row['userID'] });
                });

                const postsByUserMap = {};
                postsData.forEach(row => {
                    const userId = row['userID'];
                    if (!postsByUserMap[userId]) postsByUserMap[userId] = [];
                    postsByUserMap[userId].push(row['postID']);
                });

                const now = new Date();
                const banMap = {};
                bans.forEach(row => {
                    const username = row['username'];
                    const endDateStr = row['endDate'];
                    if (username) {
                        if (!endDateStr) banMap[username] = { reason: row['reason'], endDate: 'permanent' };
                        else {
                            const endDate = new Date(endDateStr);
                            if (!isNaN(endDate.getTime()) && endDate > now) banMap[username] = { reason: row['reason'], endDate: endDate.toISOString() };
                        }
                    }
                });

                const userMap = {};
                accounts.forEach(row => {
                    const uId = row['userID'];
                    const rawPrivacy = getColumn(row, 'profileType', 'Profile Type', 'privacy', 'profiletype') || 'public';
                    const profilePrivacy = String(rawPrivacy).trim().toLowerCase() === 'private' ? 'private' : 'public';
                    
                    const userPostIds = postsByUserMap[uId] || [];
                    let totalLikes = 0;
                    userPostIds.forEach(postId => { totalLikes += (likesByPostMap[postId] || []).length; });

                    userMap[uId] = {
                        userId: uId,
                        username: row['username'],
                        displayName: row['displayName'],
                        profilePictureUrl: row['profilePictureUrl'] || '',
                        description: row['description'] || '',
                        isVerified: row['isVerified'] || 'FALSE',
                        postVisibility: row['firePostVisibility'] || 'Everyone',
                        profilePrivacy,
                        followers: (followersMap[uId] || []).length,
                        following: (followingMap[uId] || []).length,
                        totalLikes,
                        isAdmin: String(row['isAdmin'] || 'FALSE').toUpperCase() === 'TRUE',
                        banDetails: banMap[row['username']] || null,
                        isSuspended: !!banMap[row['username']]
                    };
                });

                const user = userMap[userId];
                if (!user) throw new Error("User not found");

                let relationship = 'None';
                const currentUserFollowing = followingMap[currentUserId] || [];
                const currentUserFollowers = followersMap[currentUserId] || [];

                if (currentUserFollowing.includes(userId) && currentUserFollowers.includes(userId)) relationship = 'Friends';
                else if (currentUserFollowing.includes(userId)) relationship = 'Following';
                else if (currentUserFollowers.includes(userId)) relationship = 'Follows You';
                else if (userId === currentUserId) relationship = 'Self';
                
                user.relationship = relationship;

                return { user };
            } catch (error) {
                console.error('Error in getUserProfile TSV:', error);
                throw error;
            }
        },

        async search({ query, currentUserId }) {
            try {
                const [accounts, postsData] = await Promise.all([
                    tsvParser.fetchSheet('accounts'),
                    tsvParser.fetchSheet('posts')
                ]);
                const lowerQuery = query.toLowerCase();
                const userPrivacyMap = {};
                const users = [];

                accounts.forEach(row => {
                    const userId = row['userID'];
                    let rawPrivacy = getColumn(row, 'profileType', 'Profile Type', 'privacy', 'profiletype') || 'public';
                    const profilePrivacy = String(rawPrivacy).trim().toLowerCase() === 'private' ? 'private' : 'public';
                    userPrivacyMap[userId] = profilePrivacy;

                    const displayName = row['displayName'] || '';
                    const username = row['username'] || '';

                    if (displayName.toLowerCase().includes(lowerQuery) || username.toLowerCase().includes(lowerQuery)) {
                        users.push({
                            userId: userId,
                            displayName: displayName,
                            username: username,
                            profilePictureUrl: row['profilePictureUrl'] || '',
                            isVerified: row['isVerified'] || 'FALSE'
                        });
                    }
                });

                const posts = [];
                postsData.forEach(row => {
                    const content = row['postContent'] || '';
                    if (userPrivacyMap[row['userID']] === 'private') return;

                    if (content.toLowerCase().includes(lowerQuery)) {
                        posts.push({
                            postId: row['postID'],
                            userId: row['userID'],
                            postContent: content,
                            timestamp: row['timestamp']
                        });
                    }
                });

                return { users, posts };
            } catch (error) {
                console.error('Error in search TSV:', error);
                throw error;
            }
        }
    };

    const state = {
        currentUser: null, posts: [], currentUserFollowingList: [], currentUserFollowersList: [],
        notifications: [], unreadNotificationCount: 0, currentView: null, profileUser: null,
        backgroundPosts: null, backgroundRefreshIntervalId: null, storyUpdateIntervalId: null,
        feedScrollPosition: 0, postImageUrl: null, postVideoUrl: null, editingPostId: null, banCountdownIntervalId: null,
        scrollToPostId: null, conversations: [],
        currentConversation: { id: null, messages: [], isGroup: false, creatorId: null, members: [] },
        messagePollingIntervalId: null,
        reporting: { userId: null, postId: null },
        banningUserId: null,
        blockedUsersList: [],
        localBlocklist: new Set(),
        isConversationLoading: false,
        newChat: { selectedUsers: new Map() },
        groupEdit: { membersToAdd: new Set(), membersToRemove: new Set() },
        userProfileCache: {},
        search: { query: '', results: null, isLoading: false },
        currentFeedType: 'foryou',
        previousView: null,
        currentPostDetail: null,
        freshDataLoaded: false,
        deletedNotificationIds: new Set(),
        deletedPostIds: new Set(),
        deletedCommentIds: new Set(),
        photoLibrary: [],
        localPendingPosts: [],
        pendingOverrides: { likes: {}, follows: {} },
        pendingCommentImages: {},
        pendingCommentDrafts: {}
    };
    
    window.kangarooState = state;

    const views = {
        auth: document.getElementById('auth-view'), feed: document.getElementById('main-app-view'),
        profile: document.getElementById('profile-page-view'), editProfile: document.getElementById('edit-profile-view'),
        suspended: document.getElementById('suspended-view'), outage: document.getElementById('outage-view'),
        hashtagFeed: document.getElementById('hashtag-feed-view'), messages: document.getElementById('messages-view'),
        settings: document.getElementById('settings-view'),
        search: document.getElementById('search-view'),
        createPost: document.getElementById('create-post-view'),
        postDetail: document.getElementById('post-detail-view')
    };

    const modals = {
        notifications: document.getElementById('notifications-modal'),
        imageUrl: document.getElementById('image-url-modal'),
        videoUrl: document.getElementById('video-url-modal'),
        report: document.getElementById('report-modal'),
        profileShortcut: document.getElementById('profile-shortcut-modal'),
        newChat: document.getElementById('new-chat-modal'),
        groupSettings: document.getElementById('group-settings-modal'),
        ban: document.getElementById('ban-modal')
    };

    const VERIFIED_SVG = `<span class="material-symbols-rounded" style="color: #1DA1F2; vertical-align: -4px; margin-left: 5px;">verified</span>`;

    const applyOptimisticUpdates = (posts) => {
        const now = Date.now();
        const OVERRIDE_TIMEOUT = 120000; 

        for (const postId in state.pendingOverrides.likes) {
            if (now - state.pendingOverrides.likes[postId].timestamp > OVERRIDE_TIMEOUT) delete state.pendingOverrides.likes[postId];
        }
        for (const userId in state.pendingOverrides.follows) {
            if (now - state.pendingOverrides.follows[userId].timestamp > OVERRIDE_TIMEOUT) delete state.pendingOverrides.follows[userId];
        }

        posts.forEach(post => {
            const likeOverride = state.pendingOverrides.likes[post.postId];
            if (likeOverride) {
                const hasLike = post.likes.some(l => l.userId === state.currentUser.userId);
                if (likeOverride.status === true && !hasLike) post.likes.push({ userId: state.currentUser.userId });
                else if (likeOverride.status === false && hasLike) post.likes = post.likes.filter(l => l.userId !== state.currentUser.userId);
            }
        });

        const followOverrides = state.pendingOverrides.follows;
        for (const userId in followOverrides) {
            const override = followOverrides[userId];
            if (override.status === true) {
                if (!state.currentUserFollowingList.includes(userId)) state.currentUserFollowingList.push(userId);
            } else {
                state.currentUserFollowingList = state.currentUserFollowingList.filter(id => id !== userId);
            }
        }
        
        if (state.profileUser) {
            const followOverride = state.pendingOverrides.follows[state.profileUser.userId];
            if (followOverride) {
                const isFollowing = followOverride.status;
                const isFollower = state.currentUserFollowersList.includes(state.profileUser.userId);
                if (isFollowing && isFollower) state.profileUser.relationship = 'Friends';
                else if (isFollowing) state.profileUser.relationship = 'Following';
                else if (isFollower) state.profileUser.relationship = 'Follows You';
                else state.profileUser.relationship = 'None';
            }
        }
    };

    const api = {
        queue: [],
        isProcessingQueue: false,

        async enqueue(action, body, method = 'POST') {
            return new Promise((resolve, reject) => {
                this.queue.push({ action, body, method, resolve, reject });
                this.processQueue();
            });
        },

        async processQueue() {
            if (this.isProcessingQueue || this.queue.length === 0) return;
            this.isProcessingQueue = true;
            const request = this.queue.shift(); 

            try {
                const result = await this.callAppsScript(request.action, request.body, request.method);
                request.resolve(result);
            } catch (error) {
                console.error(`Queue error on ${request.action}:`, error);
                request.reject(error);
            } finally {
                this.isProcessingQueue = false;
                if (this.queue.length > 0) this.processQueue();
            }
        },

        async call(action, body = {}, method = 'POST') {
            const readActions = ['getPosts', 'getNotifications', 'getUserProfile', 'search', 'getConversationHistory'];

            if (readActions.includes(action) || (method === 'GET' && action !== 'login')) {
                try {
                    let result;
                    if (typeof dataAggregator[action] === 'function') {
                        result = await dataAggregator[action](body);
                    } else {
                        throw new Error(`TSV action ${action} not implemented`);
                    }
                    return result;
                } catch (tsvError) {
                    console.warn(`TSV fetch failed for ${action}, falling back:`, tsvError);
                    return await this.enqueue(action, body, method);
                }
            }
            return await this.enqueue(action, body, method);
        },

        async callAppsScript(action, body, method) {
            try {
                let url = SCRIPT_URL;
                const options = { method, mode: 'cors', redirect: 'follow' };
                if (method === 'GET') {
                    const params = new URLSearchParams({ action, ...body });
                    url += `?${params.toString()}`;
                } else {
                    options.body = JSON.stringify({ action, ...body });
                }
                const response = await fetch(url, options);
                if (!response.ok) throw new Error(`Network error: ${response.status}`);
                const result = await response.json();
                if (result.status === 'error') {
                    const error = new Error(result.message);
                    if (result.banDetails) error.banDetails = result.banDetails;
                    throw error;
                }
                return result;
            } catch (error) { console.error(`API Call failed for action "${action}":`, error); throw error; }
        }
    };

    const ui = {
        render() {
            const header = document.querySelector('header');
            const isUserLoggedIn = !['auth', 'suspended', 'outage'].includes(state.currentView);
            header.classList.toggle('hidden', !isUserLoggedIn);
            document.body.classList.toggle('logged-in', isUserLoggedIn);
            Object.values(views).forEach(v => v.classList.remove('active'));
            views[state.currentView]?.classList.add('active');

            if (state.currentView === 'feed') {
                this.renderFeed(state.posts, document.getElementById('foryou-feed'), true);
                document.getElementById('following-feed').innerHTML = '';
                document.getElementById('feed-container').style.transform = 'translateX(0)';
                document.querySelector('.feed-nav-tab[data-feed-type="foryou"]').classList.add('active');
                document.querySelector('.feed-nav-tab[data-feed-type="following"]').classList.remove('active');
                state.currentFeedType = 'foryou';
            }
            if (state.currentView === 'profile') this.renderProfilePage();
            if (state.currentView === 'editProfile') this.renderEditProfilePage();
            if (state.currentView === 'messages') this.renderMessagesPage();
            if (state.currentView === 'settings') this.renderSettingsPage();
            if (state.currentView === 'search') this.renderSearchView();
            if (state.currentView === 'postDetail') this.renderPostDetailPage();
        },
        renderPostDetailPage() {
            const container = views.postDetail.querySelector('.container');
            container.innerHTML = '';
            if (!state.currentPostDetail) {
                container.innerHTML = `<p class="error-message" style="text-align:center;">Could not load post.</p>`;
                return;
            }
            const backButton = document.createElement('a');
            backButton.className = 'back-btn';
            backButton.innerHTML = '&larr; Back';
            backButton.dataset.navBack = true;
            const postContentDiv = document.createElement('div');
            postContentDiv.id = 'post-detail-content';
            container.appendChild(backButton);
            container.appendChild(postContentDiv);
            const postElement = this.createPostElement(state.currentPostDetail, { isDetailView: true });
            postContentDiv.appendChild(postElement);
        },
        renderBanPage(banDetails) { if (state.banCountdownIntervalId) clearInterval(state.banCountdownIntervalId); const reasonEl = document.getElementById('ban-reason'); const timerContainer = document.getElementById('ban-timer-container'); const countdownEl = document.getElementById('ban-countdown'); reasonEl.textContent = banDetails.reason || 'No reason provided.'; if (banDetails.endDate === 'permanent') { timerContainer.classList.add('hidden'); return; } timerContainer.classList.remove('hidden'); const endDate = new Date(banDetails.endDate); const updateCountdown = () => { const now = new Date(); const diff = endDate - now; if (diff <= 0) { countdownEl.textContent = 'Your ban has expired. Please refresh the page.'; clearInterval(state.banCountdownIntervalId); state.banCountdownIntervalId = null; return; } const days = Math.floor(diff / (1000 * 60 * 60 * 24)); const hours = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60)); const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60)); let countdownText = ''; if (days > 0) countdownText += `${days}d `; if (hours > 0 || days > 0) countdownText += `${hours}h `; countdownText += `${minutes}m`; countdownEl.textContent = countdownText.trim(); }; updateCountdown(); state.banCountdownIntervalId = setInterval(updateCountdown, 1000); },
        renderFeed(posts, container, isMainFeed = false) {
            const validPosts = posts.filter(p => {
                const isBlocked = state.localBlocklist.has(p.userId);
                if (isBlocked) return false;
                const isStory = p.isStory === true || String(p.isStory).toUpperCase() === 'TRUE';
                if (isStory) return false;
                return true;
            });

            let postsToRender = [...validPosts];

            if (isMainFeed) {
                if (state.currentFeedType === 'following') {
                    postsToRender = postsToRender.filter(post => {
                        const isFollowed = state.currentUserFollowingList.includes(String(post.userId));
                        const isOwnPost = String(post.userId) === String(state.currentUser.userId);
                        return isFollowed || isOwnPost;
                    }).sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
                } else {
                    const now = new Date();
                    const twentyFourHoursAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
                    const stories = []; const priorityPostsWithImage = []; const priorityPostsWithoutImage = []; const otherPosts = [];
                    postsToRender.forEach(post => {
                        const isFollowed = state.currentUserFollowingList.includes(String(post.userId));
                        const isOwnPost = String(post.userId) === String(state.currentUser.userId);
                        const hasImage = (post.postContent || '').includes('<img') || (post.postContent || '').includes('<video');
                        const postDate = new Date(post.timestamp); 
                        if ((isFollowed || isOwnPost) && postDate > twentyFourHoursAgo) { 
                            if (hasImage) priorityPostsWithImage.push(post);
                            else priorityPostsWithoutImage.push(post); 
                        } else otherPosts.push(post); 
                    });
                    priorityPostsWithImage.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
                    priorityPostsWithoutImage.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
                    postsToRender = [...stories, ...priorityPostsWithImage, ...priorityPostsWithoutImage, ...otherPosts];
                }
            }

            if (!postsToRender || postsToRender.length === 0) {
                const message = isMainFeed && state.currentFeedType === 'following'
                    ? 'Posts from people you follow will appear here.'
                    : 'No posts to see here.';
                container.innerHTML = `<p style="text-align: center; color: var(--secondary-text-color); margin-top: 40px;">${message}</p>`;
                return;
            }

            container.innerHTML = '';
            postsToRender.forEach(post => container.appendChild(this.createPostElement(post, { isDetailView: false })));
        },
        renderProfilePage() {
            if (!state.profileUser) return;
            const isBlockedLocally = state.localBlocklist.has(state.profileUser.userId);

            if (state.profileUser.isSuspended || state.profileUser.banDetails) {
                const pfpUrl = sanitizeHTML(state.profileUser.profilePictureUrl) || `https://api.dicebear.com/8.x/thumbs/svg?seed=${state.profileUser.username}`;
                document.getElementById('profile-content').innerHTML = `
                    <div class="profile-header">
                        <img src="${pfpUrl}" class="pfp pfp-lg" style="filter: grayscale(100%); cursor: default;">
                        <div class="display-name" style="color: var(--secondary-text-color);">${sanitizeHTML(state.profileUser.displayName)}</div>
                        <div class="username" style="color: var(--secondary-text-color);">@${sanitizeHTML(state.profileUser.username)}</div>
                    </div>
                    <div class="private-profile-message">
                        <span class="material-symbols-rounded" style="color: var(--error-color);">block</span>
                        <h3>Account Suspended</h3>
                        <p>This account has been banned for violating our Terms of Service.</p>
                    </div>`;
                document.getElementById('profile-feed').innerHTML = '';
                return;
            }

            if (isBlockedLocally) {
                document.getElementById('profile-content').innerHTML = `<p style="text-align:center; padding: 40px; color:var(--secondary-text-color);">You have blocked this user.</p>`;
                document.getElementById('profile-feed').innerHTML = '';
                return;
            }

            const isOwnProfile = state.currentUser?.userId === state.profileUser.userId;
            const pfpUrl = sanitizeHTML(state.profileUser.profilePictureUrl) || `https://api.dicebear.com/8.x/thumbs/svg?seed=${state.profileUser.username}`;
            const postCount = state.posts.filter(p => p.userId === state.profileUser.userId).length;
            let actionButtonHTML = '';
            let optionsMenuHTML = '';

            if (!isOwnProfile) {
                const relationship = state.profileUser.relationship;
                let followButton = '';
                if (relationship === 'Friends') followButton = `<button id="follow-btn" class="secondary">Friends</button>`;
                else if (relationship === 'Following') followButton = `<button id="follow-btn" class="secondary">Unfollow</button>`;
                else followButton = `<button id="follow-btn" class="primary">Follow</button>`;

                actionButtonHTML = followButton;
                if (state.profileUser.profilePrivacy !== 'private' || relationship === 'Friends') {
                    actionButtonHTML += ` <button id="message-user-btn" class="secondary">Message</button>`;
                }

                optionsMenuHTML = `
                    <div class="profile-options-menu">
                        <button class="options-btn" title="More options"><span class="material-symbols-rounded">more_vert</span></button>
                        <div class="options-menu hidden">
                            <button data-action="report-user" data-user-id="${state.profileUser.userId}">Report User</button>
                            <button class="block-btn" data-action="block-user" data-user-id="${state.profileUser.userId}">Block User</button>
                            ${state.currentUser.isAdmin ? `<button class="delete-btn" data-action="ban-user" data-user-id="${state.profileUser.userId}">🛡️ Ban User</button>` : ''}
                        </div>
                    </div>`;
            } else {
                actionButtonHTML = `<button id="edit-profile-btn" class="secondary">Edit Profile</button>`;
            }

            const isPrivate = String(state.profileUser.profilePrivacy).trim().toLowerCase() === 'private';
            const isAuthorized = isOwnProfile || state.profileUser.relationship === 'Friends';

            const headerHTML = `
                <div class="profile-header">
                    ${optionsMenuHTML}
                    <div class="profile-grid">
                        <img src="${pfpUrl}" class="pfp pfp-lg">
                        <div class="profile-info">
                            <div class="profile-username-options">
                                <span class="profile-username">@${sanitizeHTML(state.profileUser.username)}</span>
                            </div>
                            <div class="profile-actions-ig">${actionButtonHTML}</div>
                            ${!isPrivate || isAuthorized ? `
                            <div class="profile-stats-ig" style="margin-top: 15px;">
                                <div class="stat"><span>${postCount}</span> Posts</div>
                                <div class="stat"><span>${state.profileUser.followers || 0}</span> Followers</div>
                                <div class="stat"><span>${state.profileUser.following || 0}</span> Following</div>
                            </div>` : ''}
                        </div>
                    </div>
                    <div class="profile-display-name-bio" style="padding: 0 16px;">
                        <div class="profile-display-name-ig">${sanitizeHTML(state.profileUser.displayName)} ${String(state.profileUser.isVerified).toUpperCase() === 'TRUE' ? VERIFIED_SVG : ''}</div>
                        <p class="profile-description-ig">${sanitizeHTML(state.profileUser.description || '')}</p>
                    </div>
                </div>`;

            document.getElementById('profile-content').innerHTML = headerHTML;

            if (isPrivate && !isAuthorized) {
                document.getElementById('profile-content').innerHTML += `
                    <div class="private-profile-message">
                        <span class="material-symbols-rounded">lock</span>
                        <h3>This Account is Private</h3>
                        <p>Follow this account to see their photos and videos.</p>
                    </div>`;
                document.getElementById('profile-feed').innerHTML = '';
            } else {
                this.renderFeed(state.posts.filter(p => p.userId === state.profileUser.userId), document.getElementById('profile-feed'), false);
                if (state.scrollToPostId) {
                    const postIdToScroll = state.scrollToPostId;
                    setTimeout(() => {
                        const postElement = document.querySelector(`#profile-feed .post[data-post-id="${postIdToScroll}"]`);
                        if (postElement) {
                            postElement.scrollIntoView({ behavior: 'smooth', block: 'center' });
                            postElement.style.transition = 'background-color 1s ease';
                            postElement.style.backgroundColor = `var(--warning-color)4D`;
                            setTimeout(() => { postElement.style.backgroundColor = ''; }, 2500);
                        }
                    }, 200);
                    state.scrollToPostId = null;
                }
            }
        },
        formatPostContent(content) { if (!content) return ''; let inTag = false; let formattedContent = ""; for (let i = 0; i < content.length; i++) { if (content[i] === '<') inTag = true; if (content[i] === '>') inTag = false; if (content[i] === '#' && !inTag && (i === 0 || /\s/.test(content[i - 1]))) { let tag = content.substring(i + 1).match(/^\w+/); if (tag) { formattedContent += `<a href="#" class="hashtag-link" data-hashtag="${tag[0]}">#${tag[0]}</a>`; i += tag[0].length; continue; } } formattedContent += content[i]; } return formattedContent; },
        formatCommentContent(content) {
            if (!content) return '';
            let tempDiv = document.createElement('div');
            if (content.includes('<img src="')) {
                const parts = content.split('<br>');
                let result = '';
                parts.forEach(part => {
                    if (part.startsWith('<img src="')) result += part; 
                    else { tempDiv.textContent = part; result += tempDiv.innerHTML; }
                    if (part !== parts[parts.length -1]) result += '<br>';
                });
                return result;
            }
            tempDiv.textContent = content;
            return tempDiv.innerHTML;
        },
        renderEditProfilePage() {
            document.getElementById('edit-pfp-url').value = state.currentUser.profilePictureUrl;
            document.getElementById('edit-display-name').value = state.currentUser.displayName;
            document.getElementById('edit-description').value = state.currentUser.description;
            const gallery = document.getElementById('pfp-choices-gallery');
            gallery.innerHTML = '';
            if(state.photoLibrary && state.photoLibrary.length > 0) {
                state.photoLibrary.forEach(url => {
                    const img = document.createElement('img');
                    img.src = url; img.dataset.url = url;
                    if (state.currentUser.profilePictureUrl === url) img.classList.add('selected');
                    gallery.appendChild(img);
                });
            } else gallery.innerHTML = '<p style="font-size:13px; color:var(--secondary-text-color);">No avatars available.</p>';
        },
        createPostElement(post, options = {}) {
            const { showActions = true, showComments = true, isDetailView = false } = options;
            const postDiv = document.createElement('div');
            postDiv.className = 'post';
            postDiv.dataset.postId = post.postId;
            postDiv.dataset.userId = post.userId;

            const isLiked = post.likes.some(like => like.userId === state.currentUser.userId);
            const isAuthor = post.userId === state.currentUser.userId;
            const isAdmin = state.currentUser.isAdmin;
            const pfpUrl = sanitizeHTML(post.profilePictureUrl) || `https://api.dicebear.com/8.x/thumbs/svg?seed=${post.username}`;

            let authorSpecificButtons = '';
            if (isAuthor) {
                authorSpecificButtons = `<button class="edit-btn" data-action="edit-post">Edit Post</button><button class="delete-btn" data-action="delete-post">Delete Post</button>`;
            } else {
                authorSpecificButtons = `<button data-action="report-post">Report Post</button><button class="block-btn" data-action="block-user" data-user-id="${post.userId}">Block User</button>`;
                if (isAdmin) {
                    authorSpecificButtons += `<button class="delete-btn" data-action="delete-post">🛡️ Delete Post️</button>`;
                    authorSpecificButtons += `<button class="delete-btn" data-action="ban-user" data-user-id="${post.userId}">🛡️ Ban User️</button>`;
                }
            }

            const optionsMenuHTML = `<div class="post-options"><button class="options-btn" title="More options"><span class="material-symbols-rounded">more_vert</span></button><div class="options-menu hidden">${authorSpecificButtons}</div></div>`;
            const pendingImage = state.pendingCommentImages[post.postId];
            const pendingImageHTML = pendingImage ? `<div class="comment-preview-area"><img src="${pendingImage}" class="comment-preview-image"><button class="remove-img-btn" style="align-self: flex-start; margin-left: 5px;" data-action="remove-comment-image" data-post-id="${post.postId}">&times;</button></div>` : '';
            const draftText = state.pendingCommentDrafts[post.postId] || '';

            let commentsToRender = [...post.comments].reverse();
            let viewAllBtnHTML = '';
            if (!isDetailView && commentsToRender.length > 3) {
                const totalComments = commentsToRender.length;
                commentsToRender = commentsToRender.slice(0, 3); 
                viewAllBtnHTML = `<button class="view-all-comments-btn" data-action="view-post" data-post-id="${post.postId}">View all ${totalComments} comments</button>`;
            }
            const isStory = post.isStory === true || String(post.isStory).toUpperCase() === 'TRUE';

            postDiv.innerHTML = `
                <div class="post-header"><img src="${pfpUrl}" class="pfp pfp-sm"><div class="post-header-info"><span class="post-display-name">${sanitizeHTML(post.displayName)} ${String(post.isVerified).toUpperCase() === 'TRUE' ? VERIFIED_SVG : ''}</span><span class="post-timestamp" data-timestamp="${isStory ? post.expiryTimestamp : post.timestamp}" data-is-story="${isStory}">${formatTimestamp(post)}</span></div>${optionsMenuHTML}</div>
                <div class="post-content"></div>
                ${showActions ? `<div class="post-actions"><button class="like-btn ${isLiked ? 'liked' : ''}"><span class="material-symbols-rounded">favorite</span></button><span class="like-count">${post.likes.length} likes</span></div>` : ''}
                ${showComments ? `<div class="comments-section"><div class="comments-list">${commentsToRender.map(c => this.createCommentElement(c)).join('')}${viewAllBtnHTML}</div><div class="comment-form-container">${pendingImageHTML}<form class="comment-form"><input type="text" value="${sanitizeHTML(draftText)}" placeholder="Add a comment..."><button type="button" class="comment-image-btn" title="Add Image" data-action="add-comment-image" data-post-id="${post.postId}"><span class="material-symbols-rounded">add_photo_alternate</span></button><button type="submit" class="comment-submit-btn" title="Post Comment"><span class="material-symbols-rounded">send</span></button></form></div></div>` : ''}`;

            const postContentEl = postDiv.querySelector('.post-content');
            postContentEl.innerHTML = this.formatPostContent(post.postContent);
            postContentEl.addEventListener('click', (e) => {
                if (e.target.tagName === 'A') return;
                handlers.showPostDetail(post.postId);
            });
            return postDiv;
        },
        createCommentElement(comment) {
            const isAuthor = comment.userId === state.currentUser.userId;
            const isAdmin = state.currentUser.isAdmin;
            const pfpUrl = sanitizeHTML(comment.profilePictureUrl) || `https://api.dicebear.com/8.x/thumbs/svg?seed=${comment.displayName}`;
            const optionsMenuHTML = (isAuthor || isAdmin) ? `<div class="post-options"><button class="options-btn" title="More options"><span class="material-symbols-rounded">more_vert</span></button><div class="options-menu hidden"><button class="delete-btn" data-action="delete-comment">Delete Comment</button></div></div>` : '';
            return `<div class="comment" data-comment-id="${comment.commentId}" data-user-id="${comment.userId}"><div class="comment-header"><div class="comment-header-main" data-user-id="${comment.userId}"><img src="${pfpUrl}" class="pfp"><div><a class="comment-author">${sanitizeHTML(comment.displayName)} ${String(comment.isVerified).toUpperCase() === 'TRUE' ? VERIFIED_SVG : ''}</a><span class="comment-timestamp">${formatTimestamp({ timestamp: comment.timestamp })}</span></div></div>${optionsMenuHTML}</div><div class="comment-text">${this.formatCommentContent(comment.commentText)}</div></div>`;
        },
        renderSearchView() {
            const resultsContainer = document.getElementById('search-results');
            if (state.search.isLoading) {
                resultsContainer.innerHTML = '<p style="text-align:center; color: var(--secondary-text-color);">Searching...</p>';
                return;
            }
            if (state.search.results) {
                this.renderSearchResults(state.search.results);
            } else {
                resultsContainer.innerHTML = '<p style="text-align:center; color: var(--secondary-text-color);">Search for users and posts.</p>';
            }
        },
        renderSearchResults(results) {
            const container = document.getElementById('search-results');
            if (!results.users.length && !results.posts.length) {
                container.innerHTML = '<p style="text-align:center; color: var(--secondary-text-color);">No results found.</p>';
                return;
            }
            container.innerHTML = `${results.users.length ? `<h3>Users</h3><div id="search-results-users-list"></div>` : ''}${results.posts.length ? `<h3>Posts</h3><div id="search-results-posts-list"></div>` : ''}`;
            if (results.users.length) {
                const usersList = document.getElementById('search-results-users-list');
                results.users.forEach(user => {
                    const userEl = document.createElement('div');
                    userEl.className = 'search-result-user'; userEl.dataset.userId = user.userId;
                    const pfpUrl = sanitizeHTML(user.profilePictureUrl) || `https://api.dicebear.com/8.x/thumbs/svg?seed=${user.username}`;
                    userEl.innerHTML = `<img src="${pfpUrl}" class="pfp pfp-sm"><div><div>${sanitizeHTML(user.displayName)} ${String(user.isVerified).toUpperCase() === 'TRUE' ? VERIFIED_SVG : ''}</div><div style="color:var(--secondary-text-color)">@${sanitizeHTML(user.username)}</div></div>`;
                    usersList.appendChild(userEl);
                });
            }
            if (results.posts.length) {
                const postsList = document.getElementById('search-results-posts-list');
                results.posts.forEach(post => {
                    const fullPostObject = { ...state.userProfileCache[post.userId], ...post, likes: [], comments: [] };
                    postsList.appendChild(this.createPostElement(fullPostObject, { showActions: false, showComments: false }));
                });
            }
        },
        renderNotifications() { const container = document.getElementById('notifications-list'); if (state.notifications.length === 0) { container.innerHTML = '<p style="text-align: center; color: var(--secondary-text-color); padding: 20px 0;">No new notifications.</p>'; return; } container.innerHTML = ''; state.notifications.forEach(n => { const item = document.createElement('div'); item.className = 'notification-item'; item.dataset.notificationId = n.notificationId; item.dataset.actorId = n.actorUserId; item.dataset.postId = n.postId; let text = ''; switch (n.actionType) { case 'like': text = 'liked your post.'; break; case 'comment': text = 'commented on your post.'; break; case 'follow': text = 'started following you.'; break; } const pfpUrl = sanitizeHTML(n.actorProfilePictureUrl) || `https://api.dicebear.com/8.x/thumbs/svg?seed=${n.actorDisplayName}`; item.innerHTML = ` <div class="notification-item-clickable" style="display: flex; align-items: center; gap: 12px; flex-grow: 1;"> <img src="${pfpUrl}" class="pfp pfp-sm"> <div class="notification-text"> <span class="username">${sanitizeHTML(n.actorDisplayName)}</span> ${text} <div class="notification-timestamp">${formatTimestamp({ timestamp: n.timestamp })}</div> </div> </div> <button class="delete-btn delete-notification-btn" title="Delete Notification"><span class="material-symbols-rounded">close</span></button> `; container.appendChild(item); }); },
        renderImagePreview() { 
            const container = document.getElementById('post-image-preview-container'); 
            if (state.postImageUrl) { 
                container.innerHTML = `<img id="post-image-preview" src="${sanitizeHTML(state.postImageUrl)}" alt="Image Preview"><button id="remove-image-btn" title="Remove Media">&times;</button>`; 
                container.classList.remove('hidden'); 
            } else if (state.postVideoUrl) {
                container.innerHTML = `<video id="post-video-preview" src="${sanitizeHTML(state.postVideoUrl)}" controls playsinline></video><button id="remove-image-btn" title="Remove Media">&times;</button>`; 
                container.classList.remove('hidden'); 
            } else { container.innerHTML = ''; container.classList.add('hidden'); } 
        },
        showFeedSkeleton(container) { const skeletonHTML = Array(3).fill().map(() => `<div class="post skeleton"><div class="post-header"><div class="pfp pfp-sm"></div><div class="post-display-name"></div></div><div class="post-content"></div></div>`).join(''); container.innerHTML = skeletonHTML; },
        showProfileSkeleton() { document.getElementById('profile-content').innerHTML = ` <div class="profile-header skeleton"> <div class="profile-grid"> <div class="pfp pfp-lg"></div> <div class="profile-info"> <div class="profile-username" style="width: 60%; height: 28px; margin-bottom: 15px; border-radius: 4px;"></div> <div class="profile-actions-ig" style="display: flex; gap: 8px; flex-wrap: wrap;"><div style="width: 100%; height: 32px; background-color: var(--border-color); border-radius: 10px;"></div></div> <div class="profile-stats-ig" style="margin-top: 15px; display: flex; gap: 40px;"><div class="stat" style="width: 30%; height: 32px;"></div><div class="stat" style="width: 30%; height: 32px;"></div><div class="stat" style="width: 30%; height: 32px;"></div></div> </div> </div> <div class="profile-display-name-bio" style="padding: 0 16px;"> <div class="profile-display-name-ig" style="width: 40%; height: 20px; margin-bottom: 5px; border-radius: 4px;"></div> <div class="profile-description-ig" style="width: 80%; height: 48px; border-radius: 4px;"></div> </div> </div>`; this.showFeedSkeleton(document.getElementById('profile-feed')); },
        toggleModal(modalName, show, position = null) {
            const modal = modals[modalName];
            if (!modal) return;
            modal.classList.toggle('hidden', !show);
            const modalContent = modal.querySelector('.modal-content');
            if (modalContent) { 
                if (show && position) { 
                    if (position.top !== undefined) modalContent.style.top = `${position.top}px`;
                    if (position.bottom !== undefined) modalContent.style.bottom = `${position.bottom}px`;
                    if (position.left !== undefined) modalContent.style.left = `${position.left}px`;
                    if (position.right !== undefined) modalContent.style.right = `${position.right}px`;
                } else if (!show) modalContent.removeAttribute('style'); 
            }
            if (show === false && modalName === 'report') { state.reporting = { userId: null, postId: null }; document.getElementById('report-reason-input').value = ''; }
            if (show === true && modalName === 'imageUrl') {
                const libraryContainer = document.getElementById('photo-library-grid');
                libraryContainer.innerHTML = '';
                if (state.photoLibrary && state.photoLibrary.length > 0) {
                    state.photoLibrary.forEach(url => {
                        const img = document.createElement('img');
                        img.src = url; img.className = 'library-photo';
                        img.onclick = () => {
                            if (state.pendingCommentImagePostId) {
                                state.pendingCommentImages[state.pendingCommentImagePostId] = url;
                                state.pendingCommentImagePostId = null;
                                ui.render(); 
                            } else {
                                state.postImageUrl = url;
                                state.postVideoUrl = null; 
                                ui.renderImagePreview(); 
                            }
                            ui.toggleModal('imageUrl', false);
                        };
                        libraryContainer.appendChild(img);
                    });
                } else libraryContainer.innerHTML = '<p style="font-size:13px; color:var(--secondary-text-color); padding:10px;">No photos available.</p>';
            }
        },
        renderProfileShortcutModal() {
            const user = state.currentUser;
            if (!user) return;
            const modalContent = modals.profileShortcut.querySelector('.modal-content');
            modalContent.innerHTML = `<button data-action="go-to-profile" data-user-id="${user.userId}"><span class="material-symbols-rounded">account_circle</span>View Profile</button><button data-action="go-to-settings"><span class="material-symbols-rounded">settings</span>Settings</button>`;
        },
        renderSettingsPage() {
            const user = state.currentUser;
            const pfpUrl = sanitizeHTML(user.profilePictureUrl) || `https://api.dicebear.com/8.x/thumbs/svg?seed=${user.username}`;
            const isVerified = String(user.isVerified).toUpperCase() === 'TRUE';
            document.getElementById('settings-user-info-row').innerHTML = `<a id="settings-profile-link" data-user-id="${user.userId}" style="display: flex; align-items: center; gap: 12px; text-decoration: none; color: inherit; flex-grow: 1; cursor: pointer;"><img src="${pfpUrl}" class="pfp pfp-sm"><div class="user-info"><div class="display-name">${sanitizeHTML(user.displayName)} ${isVerified ? VERIFIED_SVG : ''}</div><div class="username">@${sanitizeHTML(user.username)}</div></div></a>`;
            document.getElementById('post-visibility-select').value = state.currentUser.postVisibility || 'Everyone';
            document.getElementById('privacy-switch').checked = state.currentUser.profilePrivacy === 'private';
            const blockedListContainer = document.getElementById('blocked-users-list');
            const allBlockedIds = new Set([...state.blockedUsersList.map(u => u.userId), ...state.localBlocklist]);
            const allBlockedUsers = [];
            allBlockedIds.forEach(blockedId => {
                const existing = state.blockedUsersList.find(u => u.userId === blockedId);
                if (existing) allBlockedUsers.push(existing);
                else {
                    const cached = state.userProfileCache[blockedId] || state.posts.find(p => p.userId === blockedId);
                    if (cached) allBlockedUsers.push({ userId: blockedId, displayName: cached.displayName || 'User', profilePictureUrl: cached.profilePictureUrl || '' });
                    else allBlockedUsers.push({ userId: blockedId, displayName: 'Unknown User', profilePictureUrl: '' });
                }
            });
            if (allBlockedUsers.length > 0) {
                blockedListContainer.innerHTML = allBlockedUsers.map(blockedUser => {
                    const pfp = sanitizeHTML(blockedUser.profilePictureUrl) || `https://api.dicebear.com/8.x/thumbs/svg?seed=${blockedUser.displayName}`;
                    return `<div class="blocked-user-row setting-row"><img src="${pfp}" class="pfp pfp-sm" style="cursor:default;"><span class="blocked-user-info">${sanitizeHTML(blockedUser.displayName)}</span><button class="secondary unblock-btn" data-action="unblock-user" data-user-id="${blockedUser.userId}">Unblock</button></div>`;
                }).join('');
            } else blockedListContainer.innerHTML = '<p style="color: var(--secondary-text-color); font-size: 14px; padding: 8px 0;">You haven\'t blocked anyone.</p>';
        },
        showError(elId, msg) { const el = document.getElementById(elId); el.textContent = msg; el.classList.remove('hidden'); },
        hideError(elId) { document.getElementById(elId).classList.add('hidden'); },
        setButtonState(btnId, text, disabled) { const btn = document.getElementById(btnId); if (btn) { btn.textContent = text; btn.disabled = disabled; } },
        renderMessagesPage() { this.renderConversationsList(); if (state.currentConversation.id) { this.renderConversationHistory(); } else { document.getElementById('conversation-view').innerHTML = ` <div id="conversation-placeholder"> <span class="material-symbols-rounded">chat</span> <h3>Your Messages</h3> <p>Select a conversation or start a new one.</p> </div>`; } },
        renderConversationsList() { const container = document.getElementById('conversations-list'); let headerHTML = `<div id="conversations-list-header"><h3>Messages</h3></div>`; let listHTML = ''; if (state.conversations.length === 0) { listHTML = '<p style="text-align: center; color: var(--secondary-text-color); padding: 15px;">No conversations yet.</p>'; } else { listHTML = state.conversations.map(convo => { const isActive = convo.otherUser.userId === state.currentConversation.id; const pfp = sanitizeHTML(convo.otherUser.profilePictureUrl) || `https://api.dicebear.com/8.x/thumbs/svg?seed=${convo.otherUser.displayName}`; return ` <div class="conversation-item ${isActive ? 'active' : ''}" data-user-id="${convo.otherUser.userId}" data-is-group="${convo.otherUser.isGroup || false}"> <img src="${pfp}" class="pfp pfp-sm"> <div class="convo-details"> <div class="username">${sanitizeHTML(convo.otherUser.displayName)}</div> <div class="last-message">${sanitizeHTML(convo.lastMessage)}</div> </div> ${convo.unreadCount > 0 ? '<div class="unread-dot"></div>' : ''} </div> `; }).join(''); } container.innerHTML = headerHTML + `<div id="conversations-list-body">${listHTML}</div>`; },
        renderConversationHistory() {
            const conversationView = document.getElementById('conversation-view');
            const otherUser = state.conversations.find(c => c.otherUser.userId === state.currentConversation.id)?.otherUser;
            
            if (!otherUser) { 
                conversationView.innerHTML = `<div id="conversation-placeholder"><p>Could not load conversation.</p></div>`; 
                return; 
            }

            const pfp = sanitizeHTML(otherUser.profilePictureUrl) || `https://api.dicebear.com/8.x/thumbs/svg?seed=${otherUser.displayName}`;
            const profileLinkContent = `<img src="${pfp}" class="pfp pfp-sm"> <span>${sanitizeHTML(otherUser.displayName)} ${!otherUser.isGroup && String(otherUser.isVerified).toUpperCase() === 'TRUE' ? VERIFIED_SVG : ''}</span>`;
            const profileLink = otherUser.isGroup ? `<div>${profileLinkContent}</div>` : `<a href="#" class="profile-link" data-user-id="${otherUser.userId}">${profileLinkContent}</a>`;

            const existingHeader = document.getElementById('conversation-header');
            const existingMessagesList = document.getElementById('messages-list');
            const existingForm = document.getElementById('message-input-form');

            if (existingHeader && existingMessagesList && existingForm) {
                existingHeader.innerHTML = `<button id="back-to-convos-btn"><span class="material-symbols-rounded">arrow_back_ios_new</span></button> ${profileLink}`;
                const isScrolledToBottom = existingMessagesList.scrollHeight - existingMessagesList.scrollTop <= existingMessagesList.clientHeight + 100;
                existingMessagesList.innerHTML = state.currentConversation.messages.map(msg => this.createMessageBubble(msg)).join('');
                if (isScrolledToBottom) {
                    existingMessagesList.scrollTop = existingMessagesList.scrollHeight;
                }
            } else {
                conversationView.innerHTML = ` 
                    <div id="conversation-header"> 
                        <button id="back-to-convos-btn"><span class="material-symbols-rounded">arrow_back_ios_new</span></button> 
                        ${profileLink} 
                    </div> 
                    <div id="messages-list">
                        ${state.currentConversation.messages.map(msg => this.createMessageBubble(msg)).join('')}
                    </div> 
                    <form id="message-input-form"> 
                        <input type="text" id="message-input" placeholder="Type a message..." autocomplete="off" required> 
                        <button type="submit" class="primary">Send</button> 
                    </form> `;
                const newMessagesList = document.getElementById('messages-list');
                if (newMessagesList) {
                    newMessagesList.scrollTop = newMessagesList.scrollHeight;
                }
            }
        },
        createMessageBubble(message) {
            const isSent = message.senderId === state.currentUser.userId;
            let statusHTML = ''; 
            let wrapperContent = '';
            
            if (isSent) { 
                if (message.status === 'sent') { 
                    statusHTML = '<div class="message-status">Sent</div>'; 
                } else if (message.status === 'sending') { 
                    statusHTML = '<div class="message-status">Sending...</div>'; 
                } else if (message.status === 'failed') { 
                    statusHTML = '<div class="message-status error">Failed to send</div>'; 
                } 
                
                const optionsMenuHTML = ` 
                    <div class="message-options"> 
                        <button class="options-btn" title="More options"><span class="material-symbols-rounded">more_vert</span></button> 
                        <div class="options-menu hidden"> 
                            <button class="delete-btn" data-action="delete-message" data-message-id="${message.messageId}">Unsend</button> 
                        </div> 
                    </div> `; 
                wrapperContent = ` <div class="message-bubble sent">${sanitizeHTML(message.messageContent)}</div> ${optionsMenuHTML} `; 
            } else { 
                wrapperContent = `<div class="message-bubble received">${sanitizeHTML(message.messageContent)}</div>`; 
            }
            return `<div class="message-wrapper ${isSent ? 'sent' : 'received'}" data-message-id="${message.messageId}"> ${wrapperContent} </div> ${isSent ? statusHTML : ''}`;
        },
    };

    const handlers = {
        promptCommentImage(postId) {
            state.pendingCommentImagePostId = postId;
            ui.toggleModal('imageUrl', true);
        },
        removeCommentImage(postId) {
            delete state.pendingCommentImages[postId];
            ui.render();
        },
        clearAllNotifications() {
            const ids = state.notifications.map(n => n.notificationId);
            ids.forEach(id => state.deletedNotificationIds.add(id));
            localStorage.setItem('notificationBlacklist', JSON.stringify(Array.from(state.deletedNotificationIds)));
            state.notifications = [];
            ui.renderNotifications();
            state.unreadNotificationCount = 0;
            core.updateNotificationDot();
            ids.forEach(id => api.call('deleteNotification', { userId: state.currentUser.userId, notificationId: id }));
        },
        async login() { 
            ui.hideError('login-error'); 
            const [username, password] = [document.getElementById('login-username').value.trim(), document.getElementById('login-password').value.trim()]; 
            if (!username || !password) return ui.showError('login-error', 'All fields required.'); 
            ui.setButtonState('login-btn', 'Logging In...', true); 
            try { 
                const { user } = await api.call('login', { username, password }); 
                state.currentUser = user; 
                localStorage.setItem('currentUser', JSON.stringify(user)); 
                await core.initializeApp(); 
            } catch (e) { 
                if (e.message === 'ACCOUNT_BANNED') { 
                    ui.renderBanPage(e.banDetails); 
                    core.navigateTo('suspended'); 
                } else if (e.message === 'SERVER_OUTAGE') { 
                    core.logout(false); 
                    core.navigateTo('outage'); 
                } else { 
                    ui.showError('login-error', e.message); 
                } 
            } finally { 
                ui.setButtonState('login-btn', 'Log In', false); 
            } 
        },
        async register() {
            ui.hideError('register-error');
            const [username, displayName, password] = [
                document.getElementById('register-username').value.trim(),
                document.getElementById('register-displayname').value.trim(),
                document.getElementById('register-password').value.trim()
            ];
            const confirmPassword = document.getElementById('register-password-confirm').value.trim();
            const tosChecked = document.getElementById('register-tos').checked;
            if (!tosChecked) return ui.showError('register-error', 'You must agree to the Terms of Service.');
            if (!username || !displayName || !password || !confirmPassword) return ui.showError('register-error', 'All fields required.');
            if (password !== confirmPassword) return ui.showError('register-error', 'Passwords do not match.');
            if (password.length < 4) return ui.showError('register-error', 'Password must be at least 4 characters long.');
            if (username.length > 25) return ui.showError('register-error', 'Username cannot be longer than 25 characters.');
            if (displayName.length > 30) return ui.showError('register-error', 'Display Name cannot be longer than 30 characters.');
            if (!/^[a-zA-Z0-9_.]+$/.test(username)) return ui.showError('register-error', 'Username can only contain letters, numbers, dots, and underscores.');

            ui.setButtonState('register-btn', 'Signing Up...', true);
            try {
                const { user } = await api.call('register', { username, displayName, password });
                state.currentUser = user;
                localStorage.setItem('currentUser', JSON.stringify(user));
                await core.initializeApp();
            } catch (e) {
                ui.showError('register-error', e.message);
            } finally {
                ui.setButtonState('register-btn', 'Sign Up', false);
            }
        },
        async createPost() { 
            const contentInput = document.getElementById('post-content-input'); 
            let rawContent = contentInput.value.trim();
            let postContent = sanitizeHTML(rawContent).replace(/\n/g, '<br>'); 
            const imageUrl = state.postImageUrl; 
            const videoUrl = state.postVideoUrl; 
            
            if (!postContent && !imageUrl && !videoUrl && !state.editingPostId) return; 
            if (imageUrl) postContent += `<br><img src="${sanitizeHTML(imageUrl)}" alt="user image">`; 
            else if (videoUrl) postContent += `<br><video src="${sanitizeHTML(videoUrl)}" controls playsinline preload="metadata"></video>`;

            const isUpdating = !!state.editingPostId; 
            const button = document.getElementById('submit-post-btn'); 
            const originalButtonText = isUpdating ? 'Save Changes' : 'Post'; 
            ui.setButtonState(button.id, 'Posting...', true); 
            ui.hideError('create-post-error');

            try { 
                if (isUpdating) { 
                    await api.call('updatePost', { userId: state.currentUser.userId, postId: state.editingPostId, postContent: postContent }); 
                } else { 
                    const newPost = {
                        postId: `temp_${Date.now()}`, userId: state.currentUser.userId, postContent: postContent, isStory: false, storyDuration: 0, expiryTimestamp: null, timestamp: new Date().toISOString(), likes: [], comments: [],
                        displayName: state.currentUser.displayName, username: state.currentUser.username, profilePictureUrl: state.currentUser.profilePictureUrl, isVerified: state.currentUser.isVerified
                    };
                    state.localPendingPosts.unshift(newPost);
                    state.posts.unshift(newPost);
                    api.call('createPost', { userId: state.currentUser.userId, postContent, isStory: false, storyDuration: 0 }); 
                } 
                
                contentInput.value = ''; state.postImageUrl = null; state.postVideoUrl = null; state.editingPostId = null; 
                ui.renderImagePreview(); 
                button.textContent = 'Post'; 
                core.navigateTo('feed'); 
                if (state.currentFeedType === 'foryou') ui.renderFeed(state.posts, document.getElementById('foryou-feed'), true);
                else ui.renderFeed(state.posts, document.getElementById('following-feed'), true);
            } catch (e) { 
                 if (e.message.toLowerCase().includes("inappropriate")) ui.showError('create-post-error', e.message);
                 else alert(`Error: ${e.message}`); 
            } finally { 
                ui.setButtonState(button.id, originalButtonText, false); 
                if (isUpdating) { button.textContent = 'Post'; state.editingPostId = null; } 
            } 
        },
        enterEditMode(postId) { const post = state.posts.find(p => p.postId === postId); if (!post) return; core.navigateTo('createPost'); setTimeout(() => { const textContent = post.postContent.replace(/<br><img src=".*?" alt=".*?">/g, '').trim(); document.getElementById('post-content-input').value = textContent; document.getElementById('submit-post-btn').textContent = 'Save Changes'; state.editingPostId = postId; document.getElementById('post-content-input').focus(); }, 100); },
        async addComment(postId, commentText) { 
            if (!commentText.trim() && !state.pendingCommentImages[postId]) return;
            let finalCommentText = sanitizeHTML(commentText.trim());
            if (state.pendingCommentImages[postId]) finalCommentText += `<br><img src="${sanitizeHTML(state.pendingCommentImages[postId])}" alt="comment image">`;

            const tempComment = { 
                commentId: `temp_${Date.now()}`, userId: state.currentUser.userId, displayName: state.currentUser.displayName, isVerified: state.currentUser.isVerified, profilePictureUrl: state.currentUser.profilePictureUrl, commentText: finalCommentText, timestamp: new Date().toISOString()
            }; 
            const postIndex = state.posts.findIndex(p => p.postId === postId); 
            if (postIndex > -1) { 
                state.posts[postIndex].comments.push(tempComment); 
                delete state.pendingCommentImages[postId];
                delete state.pendingCommentDrafts[postId]; 
                ui.render(); 
            } 
            try { await api.call('addComment', { postId, userId: state.currentUser.userId, commentText: finalCommentText }); } catch (e) { 
                alert(`Error: ${e.message}`); 
                const pIndex = state.posts.findIndex(p => p.postId === postId); 
                if (pIndex > -1) { state.posts[pIndex].comments = state.posts[pIndex].comments.filter(c => c.commentId !== tempComment.commentId); ui.render(); } 
            } 
        },
        async toggleLike(postId) {
            const post = state.posts.find(p => p.postId === postId); if (!post) return;
            const isLiked = post.likes.some(l => l.userId === state.currentUser.userId);
            const newStatus = !isLiked;
            state.pendingOverrides.likes[postId] = { status: newStatus, timestamp: Date.now() };
            if (newStatus) post.likes.push({ userId: state.currentUser.userId });
            else post.likes = post.likes.filter(l => l.userId !== state.currentUser.userId);
            ui.render();
            try { await api.call('toggleLike', { postId, userId: state.currentUser.userId }); } catch (e) {
                delete state.pendingOverrides.likes[postId];
                if (!newStatus) post.likes.push({ userId: state.currentUser.userId });
                else post.likes = post.likes.filter(l => l.userId !== state.currentUser.userId);
                ui.render(); alert(`Error: ${e.message}`);
            }
        },
        async updateProfile() { ui.hideError('edit-profile-error'); const [displayName, pfpUrl, description] = [document.getElementById('edit-display-name').value, document.getElementById('edit-pfp-url').value, document.getElementById('edit-description').value]; ui.setButtonState('save-profile-btn', 'Saving...', true); try { await api.call('updateProfile', { userId: state.currentUser.userId, displayName, profilePictureUrl: pfpUrl, description }); state.currentUser = { ...state.currentUser, displayName, profilePictureUrl: pfpUrl, description }; localStorage.setItem('currentUser', JSON.stringify(state.currentUser)); await core.refreshFeed(false); await handlers.showProfile(state.currentUser.userId); } catch (e) { ui.showError('edit-profile-error', e.message); } finally { ui.setButtonState('save-profile-btn', 'Save Changes', false); } },
        async deletePost(postId) { 
            if (!confirm("Delete this post?")) return; 
            state.deletedPostIds.add(postId);
            localStorage.setItem('deletedPostIds', JSON.stringify(Array.from(state.deletedPostIds)));
            const postToDeleteIndex = state.posts.findIndex(p => p.postId === postId); 
            if (postToDeleteIndex !== -1) state.posts.splice(postToDeleteIndex, 1);
            const pendingIndex = state.localPendingPosts.findIndex(p => p.postId === postId);
            if (pendingIndex !== -1) state.localPendingPosts.splice(pendingIndex, 1);
            ui.render(); 
            try { await api.call('deletePost', { postId, userId: state.currentUser.userId }); } catch (e) { 
                alert(`Error: Could not delete post. ${e.message}`); 
                state.deletedPostIds.delete(postId); localStorage.setItem('deletedPostIds', JSON.stringify(Array.from(state.deletedPostIds))); await core.refreshFeed(true); 
            } 
        },
        async deleteComment(commentId) { 
            if (!confirm("Delete this comment?")) return; 
            state.deletedCommentIds.add(commentId);
            localStorage.setItem('deletedCommentIds', JSON.stringify(Array.from(state.deletedCommentIds)));
            let postIndex = -1, commentIndex = -1, commentToDelete = null; 
            for (let i = 0; i < state.posts.length; i++) { 
                const foundIndex = state.posts[i].comments.findIndex(c => c.commentId === commentId); 
                if (foundIndex !== -1) { postIndex = i; commentIndex = foundIndex; commentToDelete = state.posts[i].comments[foundIndex]; break; } 
            } 
            if (postIndex === -1) return; 
            state.posts[postIndex].comments.splice(commentIndex, 1); ui.render(); 
            try { await api.call('deleteComment', { commentId, userId: state.currentUser.userId }); } catch (e) { 
                alert(`Error: Could not delete comment. ${e.message}`); 
                state.deletedCommentIds.delete(commentId); localStorage.setItem('deletedCommentIds', JSON.stringify(Array.from(state.deletedCommentIds))); state.posts[postIndex].comments.splice(commentIndex, 0, commentToDelete); ui.render(); 
            } 
        },
        async toggleFollow(followingId) {
            const followBtn = document.getElementById('follow-btn'); if (!followBtn) return; followBtn.disabled = true;
            const isFollowing = state.currentUserFollowingList.includes(followingId);
            const newStatus = !isFollowing;
            state.pendingOverrides.follows[followingId] = { status: newStatus, timestamp: Date.now() };
            if (newStatus) state.currentUserFollowingList.push(followingId);
            else state.currentUserFollowingList = state.currentUserFollowingList.filter(id => id !== followingId);
            if (state.profileUser && state.profileUser.userId === followingId) {
                 const isFollower = state.currentUserFollowersList.includes(followingId);
                 if(newStatus && isFollower) state.profileUser.relationship = 'Friends';
                 else if(newStatus) state.profileUser.relationship = 'Following';
                 else if(isFollower) state.profileUser.relationship = 'Follows You';
                 else state.profileUser.relationship = 'None';
                 ui.renderProfilePage();
            }
            try { const result = await api.call('toggleFollow', { followerId: state.currentUser.userId, followingId }); if(state.profileUser && state.profileUser.userId === followingId) { state.profileUser.relationship = result.newRelationship; ui.renderProfilePage(); } } catch (e) {
                delete state.pendingOverrides.follows[followingId];
                if (!newStatus) state.currentUserFollowingList.push(followingId); else state.currentUserFollowingList = state.currentUserFollowingList.filter(id => id !== followingId);
                alert(`Error: ${e.message}`); ui.renderProfilePage();
            }
        },
        async search(query) {
            try {
                const results = await api.call('search', { query, currentUserId: state.currentUser.userId }, 'GET');
                state.search.results = results;
            } catch (e) {
                document.getElementById('search-results').innerHTML = `<p class="error-message">Search failed: ${e.message}</p>`;
                state.search.results = { users: [], posts: [] };
            } finally {
                state.search.isLoading = false;
                if (state.currentView === 'search') ui.renderSearchView();
            }
        },
        async showProfile(userId, scrollToPostId = null) {
            if (state.currentView !== 'profile') state.feedScrollPosition = window.scrollY;
            state.scrollToPostId = scrollToPostId;
            core.navigateTo('profile');
            const cachedUser = state.userProfileCache[userId];
            if (cachedUser) { state.profileUser = cachedUser; ui.renderProfilePage(); } else { state.profileUser = null; ui.showProfileSkeleton(); }
            try {
                const { user } = await api.call('getUserProfile', { userId, currentUserId: state.currentUser.userId }, 'GET');
                state.userProfileCache[userId] = { ...(state.userProfileCache[userId] || {}), ...user };
                if (state.currentView === 'profile' && (!state.profileUser || state.profileUser.userId === userId)) {
                    state.profileUser = user;
                    if (state.pendingOverrides.follows[userId]) {
                        const override = state.pendingOverrides.follows[userId];
                        const isFollower = state.currentUserFollowersList.includes(userId);
                        if (override.status && isFollower) user.relationship = 'Friends';
                        else if (override.status) user.relationship = 'Following';
                        else if (isFollower) user.relationship = 'Follows You';
                        else user.relationship = 'None';
                    }
                    ui.renderProfilePage();
                }
            } catch (e) {
                if (state.currentView === 'profile' && (!state.profileUser || state.profileUser.userId === userId)) {
                    document.getElementById('profile-content').innerHTML = `<p class="error-message" style="text-align:center;">Could not load profile: ${e.message}</p>`;
                    document.getElementById('profile-feed').innerHTML = '';
                    if (e.message === "User not found") setTimeout(() => core.navigateTo('feed'), 1500);
                }
            }
        },
        async showPostDetail(postId) {
            state.previousView = state.currentView;
            const post = state.posts.find(p => p.postId === postId);
            if (!post) { alert("Could not find post details."); return; }
            state.currentPostDetail = post;
            core.navigateTo('postDetail');
        },
        async blockUser(userIdToBlock) { 
            if (userIdToBlock === state.currentUser.userId) return; 
            const userToBlock = state.posts.find(p => p.userId === userIdToBlock) || state.profileUser || state.blockedUsersList.find(u => u.userId === userIdToBlock); 
            const userName = userToBlock ? userToBlock.displayName : 'this user'; 
            if (confirm(`Are you sure you want to block ${userName}?`)) { 
                state.localBlocklist.add(userIdToBlock);
                localStorage.setItem('localBlocklist', JSON.stringify(Array.from(state.localBlocklist)));
                state.posts = state.posts.filter(p => p.userId !== userIdToBlock);
                if (!state.blockedUsersList.find(u => u.userId === userIdToBlock) && userToBlock) state.blockedUsersList.push(userToBlock);
                ui.render(); 
                try { 
                    await api.call('blockUser', { blockerId: state.currentUser.userId, blockedId: userIdToBlock }); 
                    alert(`${userName} has been blocked.`); 
                    if (state.currentView === 'profile' && state.profileUser && state.profileUser.userId === userIdToBlock) core.navigateTo('feed'); 
                } catch (e) { alert(`Error: Could not block user. ${e.message}`); } 
            } 
        },
        async unblockUser(userIdToUnblock, event) { 
            const unblockBtn = event.target.closest('.unblock-btn'); if (!unblockBtn) return; unblockBtn.disabled = true; unblockBtn.textContent = 'Unblocking...'; 
            try { 
                state.localBlocklist.delete(userIdToUnblock);
                localStorage.setItem('localBlocklist', JSON.stringify(Array.from(state.localBlocklist)));
                state.blockedUsersList = state.blockedUsersList.filter(u => u.userId !== userIdToUnblock); 
                ui.renderSettingsPage();
                await api.call('blockUser', { blockerId: state.currentUser.userId, blockedId: userIdToUnblock }); 
            } catch (e) { alert(`Error unblocking user: ${e.message}`); state.localBlocklist.add(userIdToUnblock); ui.renderSettingsPage(); } 
        },
        openReportModal(userId, postId = null) { state.reporting = { userId, postId }; const user = state.posts.find(p => p.userId === userId) || state.profileUser; const title = document.getElementById('report-modal-title'); title.textContent = postId ? `Report post by ${user.displayName}` : `Report ${user.displayName}`; ui.toggleModal('report', true); },
        async submitReport() { const reason = document.getElementById('report-reason-input').value.trim(); if (!reason) { alert('Please provide a reason for the report.'); return; } ui.setButtonState('submit-report-btn', 'Submitting...', true); try { await api.call('reportUser', { reporterId: state.currentUser.userId, reportedId: state.reporting.userId, postId: state.reporting.postId, reason: reason }); alert('Report submitted successfully.'); ui.toggleModal('report', false); } catch (e) { alert(`Error: Could not submit report. ${e.message}`); } finally { ui.setButtonState('submit-report-btn', 'Submit Report', false); } },
        openBanModal(userId) {
            const userToBan = state.posts.find(p => p.userId === userId) || state.profileUser;
            if (!userToBan) { alert("Could not find user information."); return; }
            state.banningUserId = userId;
            document.getElementById('ban-modal-title').textContent = `Ban ${userToBan.displayName}`;
            document.getElementById('ban-reason-input').value = '';
            ui.toggleModal('ban', true);
        },
        async submitBan() {
            const reason = document.getElementById('ban-reason-input').value.trim();
            const durationHours = document.getElementById('ban-duration-select').value;
            const userIdToBan = state.banningUserId;
            if (!reason) { alert('Please provide a reason for the ban.'); return; }
            ui.setButtonState('submit-ban-btn', 'Submitting...', true);
            try {
                const result = await api.call('banUser', { adminUserId: state.currentUser.userId, bannedUserId: userIdToBan, reason: reason, durationHours: parseInt(durationHours) });
                alert(result.message || 'User has been banned successfully.');
                ui.toggleModal('ban', false); state.banningUserId = null; await core.refreshFeed(true);
            } catch (e) { alert(`Error: ${e.message}`); } finally { ui.setButtonState('submit-ban-btn', 'Submit Ban', false); }
        },
        showHashtagFeed(tag) { if (state.currentView !== 'hashtagFeed') { state.feedScrollPosition = window.scrollY; } core.navigateTo('hashtagFeed'); const titleEl = document.getElementById('hashtag-title'); const feedEl = document.getElementById('hashtag-feed'); const fullHashtag = `#${tag.toLowerCase()}`; titleEl.textContent = `Posts tagged with ${fullHashtag}`; const filteredPosts = state.posts.filter(post => (post.postContent || '').toLowerCase().includes(fullHashtag)); ui.renderFeed(filteredPosts, feedEl, false); },
        async showNotifications() { ui.toggleModal('notifications', true); ui.renderNotifications(); if (state.unreadNotificationCount > 0) { state.unreadNotificationCount = 0; core.updateNotificationDot(); try { await api.call('markNotificationsAsRead', { userId: state.currentUser.userId }); } catch (e) { console.error("Failed to mark notifications as read:", e); } } },
        async deleteNotification(notificationId) { 
            state.deletedNotificationIds.add(notificationId);
            localStorage.setItem('notificationBlacklist', JSON.stringify(Array.from(state.deletedNotificationIds))); 
            state.notifications = state.notifications.filter(n => n.notificationId !== notificationId); 
            ui.renderNotifications(); 
            try { await api.call('deleteNotification', { userId: state.currentUser.userId, notificationId }); } catch (e) { console.error("Could not delete notification: " + e.message); } 
        },
        async updatePostVisibility(e) { const newVisibility = e.target.value; const selectElement = e.target; selectElement.disabled = true; try { await api.call('updatePostVisibility', { userId: state.currentUser.userId, visibility: newVisibility }); state.currentUser.postVisibility = newVisibility; localStorage.setItem('currentUser', JSON.stringify(state.currentUser)); } catch (err) { alert('Could not update setting: ' + err.message); selectElement.value = state.currentUser.postVisibility; } finally { selectElement.disabled = false; } },
        async updateProfilePrivacy(e) { const newPrivacy = e.target.checked ? 'private' : 'public'; const switchElement = e.target; switchElement.disabled = true; try { await api.call('updateProfilePrivacy', { userId: state.currentUser.userId, privacy: newPrivacy }); state.currentUser.profilePrivacy = newPrivacy; localStorage.setItem('currentUser', JSON.stringify(state.currentUser)); } catch (err) { alert('Could not update setting: ' + err.message); switchElement.checked = state.currentUser.profilePrivacy === 'private'; } finally { switchElement.disabled = false; } },
        async loadMessagesView() { core.navigateTo('messages'); ui.renderMessagesPage(); },
        async loadConversation(otherUserId) {
            if (state.isConversationLoading) return;
            if (state.messagePollingIntervalId) clearInterval(state.messagePollingIntervalId);
            
            const convosListEl = document.getElementById('conversations-list');
            convosListEl.classList.add('is-loading');
            
            document.querySelectorAll('.conversation-item').forEach(item => {
                if (item.dataset.userId === otherUserId) {
                    item.classList.add('active');
                    const dot = item.querySelector('.unread-dot');
                    if(dot) dot.remove();
                } else {
                    item.classList.remove('active');
                }
            });

            state.isConversationLoading = true;
            state.currentConversation = { id: otherUserId, messages: [], isGroup: false };
            
            const convo = state.conversations.find(c => c.otherUser.userId === otherUserId);
            if (convo) convo.unreadCount = 0;
            
            core.updateMessageDot();
            document.querySelector('.messages-container').classList.add('show-chat-view');
            
            const view = document.getElementById('conversation-view');
            const msgList = document.getElementById('messages-list');
            
            if (msgList) {
                msgList.innerHTML = '<div style="display: flex; justify-content: center; align-items: center; height: 100%;"><p>Loading messages...</p></div>';
            } else {
                view.innerHTML = '<div id="messages-list" style="display: flex; justify-content: center; align-items: center; height: 100%;"><p>Loading messages...</p></div>';
            }

            try {
                const { messages } = await api.call('getConversationHistory', { userId: state.currentUser.userId, otherUserId, isGroup: false }, 'GET');
                state.currentConversation.messages = messages.map(m => ({ ...m, status: 'sent' }));
                ui.renderConversationHistory();
                api.call('markConversationAsRead', { userId: state.currentUser.userId, otherUserId });
                state.messagePollingIntervalId = setInterval(() => handlers.pollNewMessages(otherUserId), 3000); 
            } catch (e) {
                document.getElementById('conversation-view').innerHTML = `<div id="conversation-placeholder" class="error-message">Could not load conversation: ${e.message}</div>`;
            } finally {
                state.isConversationLoading = false;
                convosListEl.classList.remove('is-loading');
            }
        },
        async sendMessage() {
            const input = document.getElementById('message-input');
            const messageContent = input.value.trim();
            const { id: recipientId } = state.currentConversation;
            if (!messageContent || !recipientId) return;
            const tempId = `temp_${Date.now()}`;
            const tempMessage = { messageId: tempId, senderId: state.currentUser.userId, messageContent, status: 'sending', senderName: state.currentUser.displayName, timestamp: new Date().toISOString() };
            input.value = ''; input.focus();
            state.currentConversation.messages.push(tempMessage);
            ui.renderConversationHistory();
            try {
                await api.call('sendMessage', { senderId: state.currentUser.userId, recipientId, messageContent, isGroup: false });
                setTimeout(() => handlers.pollNewMessages(recipientId), 1000);
            } catch (e) {
                const messageIndex = state.currentConversation.messages.findIndex(m => m.messageId === tempId);
                if (messageIndex > -1) { state.currentConversation.messages[messageIndex].status = 'failed'; ui.renderConversationHistory(); }
            }
        },
        async deleteMessage(messageId) { 
            if (!confirm("Unsend message?")) return; 
            const messageWrapper = document.querySelector(`.message-wrapper[data-message-id="${messageId}"]`); 
            if (!messageWrapper) return; 
            messageWrapper.style.opacity = '0.5'; 
            try { 
                await api.call('deleteMessage', { userId: state.currentUser.userId, messageId }); 
                const messageIndex = state.currentConversation.messages.findIndex(m => m.messageId === messageId); 
                if (messageIndex > -1) state.currentConversation.messages.splice(messageIndex, 1); 
                messageWrapper.remove(); 
            } catch (e) { 
                alert(`Error: ${e.message}`); 
                messageWrapper.style.opacity = '1'; 
            } 
        },
        async pollNewMessages(otherUserId) {
            if (state.currentView !== 'messages' || state.currentConversation.id !== otherUserId) { if (state.messagePollingIntervalId) clearInterval(state.messagePollingIntervalId); return; }
            try {
                const { messages: remoteMessages } = await api.call('getConversationHistory', { userId: state.currentUser.userId, otherUserId, isGroup: false }, 'GET');
                const newMessagesFormatted = remoteMessages.map(m => ({ ...m, status: 'sent' }));
                const pendingMessages = state.currentConversation.messages.filter(m => m.status === 'sending' || m.status === 'failed');
                state.currentConversation.messages = [...newMessagesFormatted, ...pendingMessages];
                ui.renderConversationHistory();
                api.call('markConversationAsRead', { userId: state.currentUser.userId, otherUserId });
            } catch (e) { console.error("Polling failed:", e.message); }
        },
        async startConversationFromProfile(otherUserId) { core.navigateTo('messages'); const existingConvo = state.conversations.find(c => c.otherUser.userId === otherUserId); if (!existingConvo && state.profileUser && state.profileUser.userId === otherUserId) { const newConvo = { otherUser: { userId: state.profileUser.userId, displayName: state.profileUser.displayName, profilePictureUrl: state.profileUser.profilePictureUrl, isVerified: state.profileUser.isVerified }, lastMessage: '', timestamp: new Date().toISOString(), unreadCount: 0 }; state.conversations.unshift(newConvo); } await handlers.loadConversation(otherUserId, false); }
    };

    const core = {
        navigateTo(view) {
            if (state.currentView === 'messages' && view !== 'messages') { if (state.messagePollingIntervalId) clearInterval(state.messagePollingIntervalId); state.messagePollingIntervalId = null; state.currentConversation.id = null; document.querySelector('.messages-container').classList.remove('show-chat-view'); }
            if (state.backgroundPosts) { state.posts = state.backgroundPosts; state.backgroundPosts = null; }
            state.currentView = view;
            ui.render();
            if (view === 'feed') setTimeout(() => window.scrollTo(0, state.feedScrollPosition), 0); else window.scrollTo(0, 0);
            if (['feed', 'profile'].includes(view)) core.refreshFeed(false); 
        },
        async refreshFeed(showLoader = true) {
            if (showLoader && state.currentView === 'feed') {
                const activeFeedEl = state.currentFeedType === 'foryou' ? document.getElementById('foryou-feed') : document.getElementById('following-feed');
                ui.showFeedSkeleton(activeFeedEl);
            }
            try {
                const [postsAndConvosResult, notificationsResult] = await Promise.all([
                    api.call('getPosts', { userId: state.currentUser.userId }, 'GET'),
                    api.call('getNotifications', { userId: state.currentUser.userId }, 'GET')
                ]);
                const { posts = [], conversations = [], currentUserFollowingList = [], currentUserFollowersList = [], blockedUsersList = [], currentUserData = null, bannerText = '', photoLibrary = [] } = postsAndConvosResult || {};
                const banner = document.getElementById('global-banner');
                const root = document.documentElement;
                if (bannerText) { banner.textContent = bannerText; banner.classList.remove('hidden'); setTimeout(() => root.style.setProperty('--banner-height', `${banner.offsetHeight}px`), 0); }
                else { banner.classList.add('hidden'); root.style.setProperty('--banner-height', '0px'); }

                if (currentUserData) {
                    if (currentUserData.isSuspended === 'OUTAGE') { core.logout(false); return core.navigateTo('outage'); }
                    if (currentUserData.banDetails) { ui.renderBanPage(currentUserData.banDetails); return core.navigateTo('suspended'); }
                    state.currentUser = currentUserData;
                    state.userProfileCache[currentUserData.userId] = currentUserData;
                    localStorage.setItem('currentUser', JSON.stringify(state.currentUser));
                    const navPfp = document.getElementById('nav-pfp');
                    if (navPfp) navPfp.src = sanitizeHTML(state.currentUser.profilePictureUrl) || `https://api.dicebear.com/8.x/thumbs/svg?seed=${state.currentUser.username}`;
                    document.getElementById('logout-button-container').style.display = 'flex';
                }

                applyOptimisticUpdates(posts);

                let combinedPosts = [...posts];
                if (state.localPendingPosts && state.localPendingPosts.length > 0) {
                    const now = Date.now();
                    state.localPendingPosts = state.localPendingPosts.filter(p => (now - new Date(p.timestamp).getTime()) < 120000);
                    combinedPosts = [...state.localPendingPosts, ...combinedPosts];
                }

                state.posts = combinedPosts;
                state.conversations = conversations;
                state.currentUserFollowingList = currentUserFollowingList;
                state.currentUserFollowersList = currentUserFollowersList || [];
                state.blockedUsersList = blockedUsersList || [];
                state.photoLibrary = photoLibrary; 
                let { notifications } = notificationsResult || { notifications: [] };
                notifications = notifications.filter(n => !state.deletedNotificationIds.has(n.notificationId));
                state.notifications = notifications;
                state.unreadNotificationCount = notifications.filter(n => String(n.isRead).toUpperCase() !== 'TRUE').length;

                this.updateNotificationDot();
                this.updateMessageDot();
                state.freshDataLoaded = true;
                const messagesNavBtn = document.getElementById('messages-btn');
                if (messagesNavBtn) { messagesNavBtn.style.opacity = '1'; messagesNavBtn.style.pointerEvents = 'auto'; messagesNavBtn.title = ''; }
            } catch (e) {
                console.error("Feed refresh error:", e);
                if (state.currentView === 'feed') document.getElementById('foryou-feed').innerHTML = `<p class="error-message">Could not load feed: ${e.message}</p>`;
                if (e.message.includes("validate user session")) setTimeout(() => core.logout(), 2000);
            } finally {
                if (state.currentView === 'feed') {
                    const activeFeedEl = state.currentFeedType === 'foryou' ? document.getElementById('foryou-feed') : document.getElementById('following-feed');
                    ui.renderFeed(state.posts, activeFeedEl, true);
                    const inactiveFeedEl = state.currentFeedType === 'foryou' ? document.getElementById('following-feed') : document.getElementById('foryou-feed');
                    inactiveFeedEl.innerHTML = '';
                } else if (['profile', 'hashtagFeed', 'messages', 'settings', 'search', 'createPost', 'postDetail'].includes(state.currentView)) ui.render();
            }
        },
        updateNotificationDot() { document.getElementById('notification-dot').style.display = state.unreadNotificationCount > 0 ? 'block' : 'none'; },
        updateMessageDot() { const hasUnread = state.conversations.some(c => c.unreadCount > 0); document.getElementById('message-dot').style.display = hasUnread ? 'block' : 'none'; },
        logout(forceReload = true) { localStorage.removeItem('currentUser'); state.currentUser = null; if (state.backgroundRefreshIntervalId) clearInterval(state.backgroundRefreshIntervalId); if (state.messagePollingIntervalId) clearInterval(state.messagePollingIntervalId); if (forceReload) window.location.reload(); },
        setupEventListeners() {
            let searchTimeout;
            document.getElementById('show-register-link').addEventListener('click', () => { document.getElementById('login-form').classList.add('hidden'); document.getElementById('register-form').classList.remove('hidden'); });
            document.getElementById('show-login-link').addEventListener('click', () => { document.getElementById('register-form').classList.add('hidden'); document.getElementById('login-form').classList.remove('hidden'); });
            document.getElementById('login-btn').addEventListener('click', handlers.login); document.getElementById('register-btn').addEventListener('click', handlers.register); document.getElementById('submit-post-btn').addEventListener('click', handlers.createPost); document.getElementById('save-profile-btn').addEventListener('click', handlers.updateProfile);
            document.getElementById('logo-btn').addEventListener('click', () => core.navigateTo('feed'));
            document.getElementById('home-btn').addEventListener('click', () => core.navigateTo('feed'));
            document.getElementById('profile-nav-btn').addEventListener('click', (e) => {
                const isMobile = window.innerWidth <= 1023;
                if (isMobile) {
                    e.preventDefault(); e.stopPropagation(); ui.renderProfileShortcutModal();
                    const rect = e.currentTarget.getBoundingClientRect();
                    const bottomPos = (window.innerHeight - rect.top) + 10; 
                    const rightPos = (window.innerWidth - rect.right); 
                    const position = { bottom: bottomPos, right: rightPos < 10 ? 10 : rightPos };
                    ui.toggleModal('profileShortcut', true, position);
                } else handlers.showProfile(state.currentUser.userId);
            });
            document.getElementById('clear-notifications-btn').addEventListener('click', handlers.clearAllNotifications);
            document.getElementById('settings-nav-btn').addEventListener('click', () => core.navigateTo('settings'));
            document.getElementById('search-btn').addEventListener('click', () => core.navigateTo('search'));
            const searchInput = document.getElementById('search-input');
            if (searchInput) {
                searchInput.addEventListener('input', (e) => {
                    clearTimeout(searchTimeout);
                    const query = e.target.value.trim();
                    state.search.query = query;
                    if (query.length === 0) { state.search.results = null; state.search.isLoading = false; ui.renderSearchView(); } 
                    else if (query.length >= 2) { state.search.isLoading = true; ui.renderSearchView(); searchTimeout = setTimeout(() => handlers.search(query), 300); }
                });
            }
            document.getElementById('notifications-btn').addEventListener('click', handlers.showNotifications);
            document.getElementById('messages-btn').addEventListener('click', () => handlers.loadMessagesView());
            document.querySelectorAll('.close-modal-btn').forEach(btn => btn.addEventListener('click', (e) => { const modal = e.target.closest('.modal'); if (modal) { const modalName = modal.id.replace('-modal', '').replace(/-(\w)/g, (match, p1) => p1.toUpperCase()); ui.toggleModal(modalName, false); } }));
            document.getElementById('logout-btn').addEventListener('click', core.logout);
            document.getElementById('outage-logout-btn').addEventListener('click', core.logout);
            document.getElementById('theme-switch').addEventListener('change', (e) => { const newTheme = e.target.checked ? 'dark' : 'light'; document.documentElement.setAttribute('data-theme', newTheme); localStorage.setItem('theme', newTheme); });
            document.getElementById('back-to-profile-btn').addEventListener('click', () => { handlers.showProfile(state.profileUser.userId) });
            document.getElementById('post-visibility-select').addEventListener('change', handlers.updatePostVisibility);
            document.getElementById('privacy-switch').addEventListener('change', handlers.updateProfilePrivacy);
            document.getElementById('add-image-btn').addEventListener('click', () => ui.toggleModal('imageUrl', true));
            document.getElementById('image-url-modal-done-btn').addEventListener('click', () => { const url = document.getElementById('image-url-modal-input').value.trim(); if (url) { if (state.pendingCommentImagePostId) { state.pendingCommentImages[state.pendingCommentImagePostId] = url; state.pendingCommentImagePostId = null; ui.render(); } else { state.postImageUrl = url; state.postVideoUrl = null; ui.renderImagePreview(); } } document.getElementById('image-url-modal-input').value = ''; ui.toggleModal('imageUrl', false); });
            document.getElementById('add-video-btn').addEventListener('click', () => ui.toggleModal('videoUrl', true));
            document.getElementById('video-url-modal-done-btn').addEventListener('click', () => { const url = document.getElementById('video-url-modal-input').value.trim(); if (url) { state.postVideoUrl = url; state.postImageUrl = null; ui.renderImagePreview(); } document.getElementById('video-url-modal-input').value = ''; ui.toggleModal('videoUrl', false); });
            document.getElementById('register-tos').addEventListener('change', (e) => { document.getElementById('register-btn').disabled = !e.target.checked; });
            document.getElementById('submit-report-btn').addEventListener('click', handlers.submitReport);
            document.getElementById('submit-ban-btn').addEventListener('click', handlers.submitBan);
            document.getElementById('create-post-form').addEventListener('click', (e) => { if (e.target.id === 'remove-image-btn') { state.postImageUrl = null; state.postVideoUrl = null; ui.renderImagePreview(); } });
            document.getElementById('notifications-list').addEventListener('click', (e) => { const item = e.target.closest('.notification-item'); if (!item) return; if (e.target.closest('.delete-notification-btn')) { handlers.deleteNotification(item.dataset.notificationId); return; } const clickableArea = e.target.closest('.notification-item-clickable'); if (clickableArea) { ui.toggleModal('notifications', false); const notification = state.notifications.find(n => n.notificationId === item.dataset.notificationId); if (!notification) return; if (notification.postAuthorId && notification.postId && notification.postId !== 'null') handlers.showProfile(notification.postAuthorId, notification.postId); else if (notification.actorUserId) handlers.showProfile(notification.actorUserId); } });
            document.getElementById('edit-profile-view').addEventListener('click', e => { const choice = e.target.closest('#pfp-choices-gallery img'); if (choice) { document.getElementById('edit-pfp-url').value = choice.dataset.url; document.querySelectorAll('#pfp-choices-gallery img').forEach(img => img.classList.remove('selected')); choice.classList.add('selected'); } });
            document.body.addEventListener('input', (e) => { if (e.target.matches('.comment-form input')) { const postId = e.target.closest('.post').dataset.postId; state.pendingCommentDrafts[postId] = e.target.value; } });
            document.querySelectorAll('.feed-nav-tab').forEach(tab => {
                tab.addEventListener('click', () => {
                    const feedType = tab.dataset.feedType;
                    if (state.currentFeedType === feedType) return;
                    state.currentFeedType = feedType;
                    document.querySelectorAll('.feed-nav-tab').forEach(t => t.classList.remove('active')); tab.classList.add('active');
                    const container = document.getElementById('feed-container');
                    if (feedType === 'following') { container.style.transform = 'translateX(-50%)'; const followingFeedEl = document.getElementById('following-feed'); if (followingFeedEl.innerHTML.trim() === '') ui.renderFeed(state.posts, followingFeedEl, true); } 
                    else { container.style.transform = 'translateX(0)'; const foryouFeedEl = document.getElementById('foryou-feed'); if (foryouFeedEl.innerHTML.trim() === '') ui.renderFeed(state.posts, foryouFeedEl, true); }
                });
            });
            document.getElementById('open-create-post-btn').addEventListener('click', () => core.navigateTo('createPost'));
            document.body.addEventListener('click', (e) => {
                const target = e.target;
                const backBtn = target.closest('[data-nav-back]');
                if (backBtn) { e.preventDefault(); core.navigateTo(state.previousView || 'feed'); return; }
                if (!modals.profileShortcut.classList.contains('hidden') && !target.closest('#profile-shortcut-modal .modal-content') && !target.closest('#profile-nav-btn')) ui.toggleModal('profileShortcut', false);
                const optionsBtn = target.closest('.options-btn');
                if (optionsBtn) { e.preventDefault(); const menu = optionsBtn.nextElementSibling; const isHidden = menu.classList.contains('hidden'); document.querySelectorAll('.options-menu').forEach(m => m.classList.add('hidden')); if (isHidden) menu.classList.remove('hidden'); return; }
                if (!target.closest('.options-menu')) document.querySelectorAll('.options-menu').forEach(m => m.classList.add('hidden'));
                const actionButton = target.closest('[data-action]');
                if (actionButton) {
                    const profileShortcutModal = target.closest('#profile-shortcut-modal');
                    if (profileShortcutModal) {
                        const action = actionButton.dataset.action;
                        if (action === 'go-to-profile') { e.preventDefault(); const userId = actionButton.dataset.userId; ui.toggleModal('profileShortcut', false); handlers.showProfile(userId); }
                        else if (action === 'go-to-settings') { e.preventDefault(); ui.toggleModal('profileShortcut', false); core.navigateTo('settings'); }
                        return;
                    }
                    const action = actionButton.dataset.action; const postEl = actionButton.closest('.post'); const commentEl = actionButton.closest('.comment');
                    if (action === 'delete-post') handlers.deletePost(postEl.dataset.postId);
                    else if (action === 'delete-comment') handlers.deleteComment(commentEl.dataset.commentId);
                    else if (action === 'edit-post') handlers.enterEditMode(postEl.dataset.postId);
                    else if (action === 'delete-message') handlers.deleteMessage(actionButton.dataset.messageId);
                    else if (action === 'block-user') handlers.blockUser(actionButton.dataset.userId);
                    else if (action === 'unblock-user') handlers.unblockUser(actionButton.dataset.userId, e);
                    else if (action === 'report-post') handlers.openReportModal(postEl.dataset.userId, postEl.dataset.postId);
                    else if (action === 'report-user') handlers.openReportModal(actionButton.dataset.userId);
                    else if (action === 'ban-user') handlers.openBanModal(actionButton.dataset.userId);
                    else if (action === 'add-comment-image') handlers.promptCommentImage(actionButton.dataset.postId);
                    else if (action === 'remove-comment-image') handlers.removeCommentImage(actionButton.dataset.postId);
                    else if (action === 'view-post') handlers.showPostDetail(actionButton.dataset.postId);
                    return;
                }
                if (target.closest('[data-nav="feed"]')) { e.preventDefault(); return core.navigateTo('feed'); }
                const hashtagLink = target.closest('.hashtag-link'); if (hashtagLink) { e.preventDefault(); handlers.showHashtagFeed(hashtagLink.dataset.hashtag); return; }
                const profileLink = target.closest('.post-header, .comment-header-main, .search-result-user, #conversation-header .profile-link, #settings-profile-link');
                if (profileLink && !target.closest('.post-options')) {
                    const userElement = profileLink.closest('[data-user-id]');
                    if (userElement && userElement.dataset.userId) {
                        if (state.currentView === 'search') { document.getElementById('search-input').value = ''; state.search = { query: '', results: null, isLoading: false }; }
                        handlers.showProfile(userElement.dataset.userId); return;
                    }
                }
                if (target.closest('#edit-profile-btn')) return core.navigateTo('editProfile');
                if (target.closest('#follow-btn')) return handlers.toggleFollow(state.profileUser.userId);
                const likeBtn = target.closest('.like-btn'); if (likeBtn) return handlers.toggleLike(target.closest('.post').dataset.postId);
                if (target.closest('#message-user-btn')) return handlers.startConversationFromProfile(state.profileUser.userId);
                const conversationItem = target.closest('.conversation-item'); if (conversationItem) { const otherUserId = conversationItem.dataset.userId; if (otherUserId && otherUserId !== state.currentConversation.id) handlers.loadConversation(otherUserId); return; }
                const backToConvosBtn = target.closest('#back-to-convos-btn'); if (backToConvosBtn) { document.querySelector('.messages-container').classList.remove('show-chat-view'); if (state.messagePollingIntervalId) clearInterval(state.messagePollingIntervalId); state.messagePollingIntervalId = null; state.currentConversation.id = null; }
            });
            document.body.addEventListener('submit', (e) => { 
                const commentForm = e.target.closest('.comment-form'); 
                if (commentForm) { e.preventDefault(); const input = commentForm.querySelector('input'); const postId = e.target.closest('.post').dataset.postId; handlers.addComment(postId, input.value); input.value = ''; delete state.pendingCommentDrafts[postId]; } 
                const messageForm = e.target.closest('#message-input-form'); 
                if (messageForm) { e.preventDefault(); handlers.sendMessage(); } 
            });
            document.getElementById('auth-view').addEventListener('keydown', (e) => { if (e.key !== 'Enter') return; const activeForm = !document.getElementById('login-form').classList.contains('hidden') ? document.getElementById('login-form') : document.getElementById('register-form'); e.preventDefault(); const inputs = [...activeForm.querySelectorAll('input')]; const currentInputIndex = inputs.findIndex(input => input === document.activeElement); if (currentInputIndex > -1 && currentInputIndex < inputs.length - 1) inputs[currentInputIndex + 1].focus(); else activeForm.querySelector('button.primary').click(); });
        },
        async initializeApp() { 
            const savedUser = JSON.parse(localStorage.getItem('currentUser')); 
            if (!savedUser) { return core.navigateTo('auth'); } 
            state.currentUser = savedUser; 
            state.deletedNotificationIds = new Set(JSON.parse(localStorage.getItem('notificationBlacklist') || '[]'));
            state.deletedPostIds = new Set(JSON.parse(localStorage.getItem('deletedPostIds') || '[]'));
            state.deletedCommentIds = new Set(JSON.parse(localStorage.getItem('deletedCommentIds') || '[]'));
            state.localBlocklist = new Set(JSON.parse(localStorage.getItem('localBlocklist') || '[]'));
            const navPfp = document.getElementById('nav-pfp');
            if (navPfp && state.currentUser.profilePictureUrl) navPfp.src = sanitizeHTML(state.currentUser.profilePictureUrl);
            core.navigateTo('feed'); 
            ui.showFeedSkeleton(document.getElementById('foryou-feed')); 
            try { await core.refreshFeed(false); } catch (error) { alert(`Session error: ${error.message}. Please log in again.`); core.logout(); } 
        },
        main() { core.setupEventListeners(); const savedTheme = localStorage.getItem('theme') || 'dark'; document.documentElement.setAttribute('data-theme', savedTheme); document.getElementById('theme-switch').checked = savedTheme === 'dark'; if (localStorage.getItem('currentUser')) { core.initializeApp(); } else { core.navigateTo('auth'); } }
    };

    const formatTimestamp = (post) => {
        const timestampStr = post.timestamp;
        if (!timestampStr) return '';
        if (typeof timestampStr === 'string' && timestampStr.trim() === '') return '';
        
        const date = new Date(timestampStr);
        if (isNaN(date.getTime())) return '';

        const now = new Date();
        const secondsAgo = Math.round((now - date) / 1000);
        if (secondsAgo < 60) return 'just now';
        const minutesAgo = Math.round(secondsAgo / 60);
        if (minutesAgo < 60) return `${minutesAgo}m ago`;
        const hoursAgo = Math.round(minutesAgo / 60);
        if (hoursAgo < 24) return `${hoursAgo}h ago`;
        const daysAgo = Math.round(hoursAgo / 24);
        if (daysAgo <= 14) return `${daysAgo}d ago`;
        return date.toLocaleDateString('en-us', { month: 'short', day: 'numeric' });
    };
    
    const sanitizeHTML = (str) => { if (!str) return ''; const temp = document.createElement('div'); temp.textContent = str; return temp.innerHTML; };
    
    core.main();
});
