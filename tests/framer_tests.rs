use bytes::{Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use MMA::core::commands::{Response, ResponseOpcode};
use MMA::core::config::FramerConfig;
use MMA::protocol::framer::Framer;

fn response(payload: &[u8], options: Vec<(&str, &str)>) -> Response {
    let mut value = Response::new(0x1020_3040);
    value.headers.opcode = ResponseOpcode::Ok;
    value.headers.version = 7;
    value.payload = Bytes::copy_from_slice(payload);
    value.options = options
        .into_iter()
        .map(|(key, value)| {
            (
                Bytes::copy_from_slice(key.as_bytes()),
                Bytes::copy_from_slice(value.as_bytes()),
            )
        })
        .collect();
    value
}


fn round_trip(config: FramerConfig, item: Response) -> MMA::core::commands::Request {
    let expected_payload = item.payload.clone();
    let expected_options = item.options.clone();
    let mut encoder = Framer::new(config.clone());
    let mut bytes = BytesMut::new();
    encoder.encode(item, &mut bytes).unwrap();

    let mut decoder = Framer::new(config);
    let request = decoder.decode(&mut bytes).unwrap().unwrap();
    assert_eq!(request.payload, expected_payload);
    assert_eq!(request.options, expected_options);
    request
}

#[test]
fn round_trip_preserves_payload_headers_and_options() {
    let request = round_trip(
        FramerConfig::new(),
        response(
            b"hello",
            vec![("content-type", "text/plain"), ("x-id", "42")],
        ),
    );

    assert_eq!(request.headers.version, 7);
    assert_eq!(request.headers.req_id, 0x1020_3040);
    assert_eq!(request.route, "");
}

#[test]
fn round_trip_supports_all_valid_body_orders() {
    for (route_order, payload_order, options_order) in [
        (1, 2, 3),
        (1, 3, 2),
        (2, 1, 3),
        (2, 3, 1),
        (3, 1, 2),
        (3, 2, 1),
    ] {
        let mut config = FramerConfig::new();
        config.route_order = route_order;
        config.payload_order = payload_order;
        config.options_order = options_order;
        let request = round_trip(config, response(b"payload", vec![("k", "v")]));
        assert_eq!(request.payload, Bytes::from_static(b"payload"));
        assert_eq!(request.options, vec![("k".into(), "v".into())]);
    }
}

#[test]
fn decoder_waits_for_a_complete_frame_and_handles_one_byte_chunks() {
    let config = FramerConfig::new();
    let mut encoder = Framer::new(config.clone());
    let mut encoded = BytesMut::new();
    encoder
        .encode(response(b"partial", vec![("a", "b")]), &mut encoded)
        .unwrap();

    let mut decoder = Framer::new(config);
    let mut input = BytesMut::new();
    for byte in encoded {
        input.extend_from_slice(&[byte]);
        if input.len() < 29 + 7 + 6 {
            assert!(decoder.decode(&mut input).unwrap().is_none());
        }
    }
    let request = decoder.decode(&mut input).unwrap().unwrap();
    assert_eq!(request.payload, Bytes::from_static(b"partial"));
}

#[test]
fn encoder_rejects_empty_and_oversized_options_without_mutating_destination() {
    let mut framer = Framer::new(FramerConfig::new());
    let mut dst = BytesMut::from(&b"prefix"[..]);
    let original = dst.clone();
    assert!(
        framer
            .encode(response(b"x", vec![("", "v")]), &mut dst)
            .is_err()
    );
    assert_eq!(dst, original);

    let huge = "x".repeat(u16::MAX as usize + 1);
    assert!(
        framer
            .encode(response(b"x", vec![(&huge, "v")]), &mut dst)
            .is_err()
    );
    assert_eq!(dst, original);
}

#[test]
fn encoder_rejects_too_many_options_and_message_too_large() {
    let mut framer = Framer::new(FramerConfig::new());
    let mut dst = BytesMut::new();
    let options = (0..=u8::MAX).map(|_| ("k".into(), "v".into())).collect();
    let mut item = response(b"x", vec![]);
    item.options = options;
    assert!(framer.encode(item, &mut dst).is_err());
    assert!(dst.is_empty());

    let mut config = FramerConfig::new();
    config.max_message_length = 2;
    let mut framer = Framer::new(config);
    assert!(framer.encode(response(b"123", vec![]), &mut dst).is_err());
    assert!(dst.is_empty());
}

#[test]
fn decoder_rejects_body_over_limit_before_waiting_for_body() {
    let mut config = FramerConfig::new();
    config.max_message_length = 3;
    let mut frame = BytesMut::zeroed(config.header_length as usize);
    frame[config.opcode_pos as usize - 1] = 1;
    frame[config.header_length as usize - 8..config.header_length as usize - 4]
        .copy_from_slice(&4u32.to_be_bytes());
    let mut decoder = Framer::new(config);
    assert!(decoder.decode(&mut frame).is_err());
}
