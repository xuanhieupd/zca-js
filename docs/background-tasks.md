# Background tasks của Zalo Web và cách fork này hiện thực

Tài liệu này ghi lại kết quả dịch ngược bundle Zalo Web (`chat.zalo.me`, build `26.11.10`, `zpw_type=30`, `zpw_ver=695`) và phần đã đưa vào `Listener` của zca-js.

## 1. Vòng đời websocket

| Bước | Zalo Web | Fork này |
|---|---|---|
| Mở kết nối | `establish_timeout` = 15 s, quá hạn thì đóng với mã nội bộ `5012` rồi retry | `CloseReason.EstablishTimeout` |
| Chờ xác thực | `timeout_authen` = 15 s, quá hạn thì `5015` | `CloseReason.AuthTimeout` |
| Frame xác thực `cmd 1 / subCmd 1` | Cùng một frame chứa `key` (khóa AES-GCM) và `data` đã mã hóa bằng chính khóa đó. `data.qCmds` là danh sách hàng đợi offline | Giải mã `data`, đọc `qCmds` |
| Sau xác thực | Reset bộ đếm retry của các lỗi đã hồi phục, ping ngay, rồi kéo hàng đợi offline | Reset toàn bộ bộ đếm retry (bản gốc không bao giờ reset nên dần hết lượt retry) |

Mã đóng nội bộ không phải mã websocket hợp lệ, nên Zalo Web tự đóng socket rồi retry theo bảng `settings.features.socket.retries[mã]`. Fork dùng `ws.terminate()`, sau đó phát `disconnected` / `closed` với mã nội bộ. Retry chỉ chạy khi gọi `start({ retryOnClose: true })`.

## 2. Heartbeat (ping/pong)

- Gửi `cmd 2 / subCmd 1` (`KEEP_CONNECTION`) với `{ eventId: Date.now() }` mỗi `ping_interval` (mặc định 60 s), và gửi một lần ngay sau xác thực.
- Server trả pong ở `cmd 2`, kèm `eventId`. Nếu sau 5 s chưa có pong thì ping lại theo bảng `retries["5013"]` (`PING_PONG`). Hết lượt thì đóng với `5011` (`PING_TIMEOUT`).
- Nếu lần pong cuối cách hơn `ping_max_timeout` (300 s) thì đóng với `5016` (`ABNORMAL_STOP_PING`).
- Bản gốc chỉ gửi ping mà không kiểm tra pong, nên socket chết im lặng không bao giờ được phát hiện.

## 3. Hàng đợi offline (`qCmds`)

- Mỗi hàng đợi có khóa `${cmd}_${subCmd}`, ví dụ `510_1` là tin 1-1, `511_1` là tin nhóm, `603_0` là control, `610_0` / `611_0` là reaction.
- Yêu cầu: `{ cmd, subCmd, data: { first, reqId, lastId, preIds } }`. `lastId` là action id cuối cùng đã nhận của hàng đợi đó.
- Phản hồi trả về cùng `cmd`: `{ msgs | groupMsgs | controls | reacts, more, lastActionId, queueStatus }`. Khi `more` bật thì gửi tiếp với `first: false, lastId: lastActionId`.
- Toàn bộ quá trình kéo phải xong trong `offline_timeout` (480 s), nếu không thì đóng với `5014`.
- Mọi gói push online (`501`, `521`, `601`, `612`…) đều kèm `queueStatus`, từ đó Zalo Web cập nhật con trỏ `lastId` cho từng hàng đợi.

Fork này:
- Lưu con trỏ qua `QueueCursorStore` (mặc định ở bộ nhớ, có thể truyền store bền vững của riêng bạn). Con trỏ chỉ tăng, so sánh bằng BigInt.
- Chỉ kéo những hàng đợi đã có con trỏ. Kéo từ `lastId = 1` sẽ phát lại cả lịch sử như tin mới, và bot sẽ trả lời nhầm.
- Bỏ qua các hàng đợi E2EE (`513`–`519`, `713`, `715`).
- Tin offline được phát qua các sự kiện thường (`message`, `undo`, `reaction`, `group_event`, `friend_event`); kéo xong thì phát `offline_done`.

## 4. Tin bị sót: `HAS_MSG` → `GET_MSG`

- Server có thể chỉ báo `cmd 533` `{ queueId: { cmd, subCmd }, ids: [{ actionId, msgId }] }` thay vì đẩy nguyên tin.
- Client gom các báo trong 10 ms (`delay_get_msg`), rồi gửi `cmd 570` `{ queueId, ids (tối đa 20 / lần), lastId, reqId }`.
- Tin trả về ở `cmd 570` hoặc `534`. Id nào sau 3 s chưa về thì gửi lại, tối đa 4 lần.
- Bản gốc bỏ qua `533`, nên những tin này mất hẳn.

## 5. Đối chiếu tin: `VERIFY_MSG`

- Với các hàng đợi tin trong `qCmds`, client giữ danh sách action id nhận được (lấy từ `queueStatus[q].ids`).
- Gửi `cmd 532` `{ nretry, queueName, ids: [{ actionId }] (tối đa 20), reqId }` khi:
  - đủ 22 id, hoặc
  - sau 30 s (`interval_verify`).
- Phản hồi `{ error, reqId }`:
  - `0`: xong. Bỏ các id đã đối chiếu, giữ id cuối làm mốc.
  - `1`: có tin bị sót. Server đẩy tin sót qua `534` kèm `reqId`. Nếu sau 30 s (`timeout_retry_miss`) vẫn chưa thấy thì đối chiếu lại với `nretry + 1`.
  - `-1`: dừng.
- Nếu đang kéo offline thì chỉ bắt đầu đối chiếu sau khi kéo xong (`verify_on_done_offline`).

## 6. HTTP keepalive và làm mới cấu hình

- `GET {chat}/keepalive?params=AES({ imei })` mỗi `settings.keepalive.keepalive_duration` giây (mặc định 120).
- Phản hồi có `config_vesion`. Nếu lớn hơn giá trị đã biết thì Zalo Web gọi lại `getServerInfo` và nạp lại `settings`. Ngoài ra Zalo Web tự gọi `getServerInfo` mỗi giờ.
- Fork phát `config_version`, gọi lại `getServerInfo`, gán `ctx.settings` mới và phát `settings_updated`.

## 7. Trạng thái hoạt động (tùy chọn)

- `cmd 4` (`PING_ACTIVE`) với `{ at: ActiveStatus }` mỗi `online_configs.update_action_interval` (mặc định 60 s).
- Trạng thái đầu tiên là `FirstActiveForeground (2)`, các lần sau là `KeepActiveForeground (1)`.
- Zalo Web chỉ gửi khi người dùng có thao tác. Bot bật `activeStatus` thì sẽ luôn hiện là đang hoạt động.

## 8. Tác vụ định kỳ khác (chưa hiện thực, có API sẵn để tự gọi)

| Tác vụ | Chu kỳ | Endpoint |
|---|---|---|
| Danh sách tắt thông báo | 1 giờ | `profile /api/social/profile/getmute` |
| Gợi ý kết bạn / lời mời | 7200 s | `friend /api/friend/recommendsv2/list` |
| Hồ sơ của chính mình | 24 giờ (khi đã quá 20 giờ) | `profile /api/social/profile/me-v2` |
| Đối chiếu danh sách bạn | 24 giờ, server bật bằng `enable_api_verify_fr_list` | `profile /api/social/friend/verify` |
| Tải lại toàn bộ danh sách bạn | 36 giờ | `profile /api/social/friend/getfriends` |
| Alias | 24 giờ (v2: 7 ngày) | `alias /api/alias/list` |
| Danh bạ | 24 giờ khi version tăng | `profile /api/social/friend/phonebook/v2` |
| Hội thoại đã ghim | 24 giờ | `conversation /api/pinconvers/list` |
| Gửi delivered | 30 phút | `reaction /api/message/react_delivered` |
| Long-poll dự phòng | liên tục khi websocket hỏng | `zpw_chts_new` / `zpw_ctrs_new` |

## Sử dụng

```ts
const zalo = new Zalo({
    backgroundTasks: {
        keepAlive: true,
        syncOffline: true,
        fetchMissingMessages: true,
        verifyMessages: true,
        refreshServerInfo: true,
        activeStatus: false,
        queueCursorStore: myPersistentStore,
    },
});
```

Mặc định: `keepAlive`, `fetchMissingMessages`, `verifyMessages`, `refreshServerInfo` bật; `syncOffline`, `activeStatus` tắt.

Sự kiện mới: `offline_done`, `config_version`, `settings_updated`. Mã đóng mới: `CloseReason.PingTimeout`, `EstablishTimeout`, `OfflineTimeout`, `AuthTimeout`, `AbnormalStopPing`.
