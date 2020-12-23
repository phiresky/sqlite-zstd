use std::io::{Cursor, Read};
use std::{fs::File, io::Write};

fn decode_dict(enc: &[u8], dict: &[u8]) -> Vec<u8> {
    let mut dec = Vec::new();
    let mut decoder = zstd::stream::write::Decoder::with_dictionary(&mut dec, dict).unwrap();
    decoder.write_all(&enc).unwrap();
    decoder.flush().unwrap();
    decoder.into_inner();
    dec
}
fn main() {
    let dict = include_bytes!("../../test/dict");

    let data = include_bytes!("../../test/eg.json");

    let encoded = zstd::encode_all(&data[..], 19).unwrap();

    let mut out = Vec::new();

    let cdict = zstd::dict::EncoderDictionary::new(dict, 19);

    println!("------------------- BEGIN BROKEN ----------------------");

    let mut encoded_with_dict = zstd::Encoder::with_prepared_dictionary(&mut out, &cdict).unwrap();

    encoded_with_dict
        .set_pledged_src_size(data.len() as u64)
        .unwrap();
    encoded_with_dict.write_all(&data[..]).unwrap();
    encoded_with_dict.finish().unwrap();

    println!("------------------- BEGIN WORKING ----------------------");
    let simple_encoded = {
        let dict = zstd_safe::CDict::create(dict, 19);

        let mut out = vec![0; 10_000_000];

        let mut c = zstd_safe::CCtx::create();

        let encoded_with_dict = c
            .compress_using_cdict(&mut out, data, &dict)
            .map_err(zstd_safe::get_error_name)
            .unwrap();
        out[0..encoded_with_dict].to_vec()
    };
    println!("--------------------- SANITY CHECK -----------------------");

    let dec1 = decode_dict(&simple_encoded, dict);
    let dec2 = decode_dict(&out, dict);
    assert!(dec1 == data);
    assert!(dec2 == data);

    println!(
        "without dict {} bytes, with dict {} bytes, simple api with dict {} bytes",
        encoded.len(),
        out.len(),
        simple_encoded.len()
    );
}
